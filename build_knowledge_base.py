#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地笔记库 → Open WebUI Knowledge Collection（增量同步）

扫描：
  <VAULT>/00-长期记忆/**/*.md
  <VAULT>/20-通用知识/**/*.md
  <VAULT>/30-知识资产/**/*.md
  <VAULT>/40-技能库/**/*.md
  （历史归档目录主动排除，以保持检索信噪比）

动作：
  1. 在 Open WebUI 创建 / 复用 Knowledge Collection
  2. 把每个 md 文件上传为 File 并生成 embedding
  3. 把 File 加入 Collection

处理方式（大目录批量同步的关键）：
  - 上传时 process_in_background=true，避免同步 embedding 堆积文件句柄
  - 轮询 /api/v1/files/{id}/process/status 到 completed / failed
  - 内容变更时先移除旧 File 再传新的，避免知识库里累积重复向量
  - 每个请求独立连接（Connection: close），减少句柄占用
  - 全程状态留痕，中断后可续传

用法：
  python3 build_knowledge_base.py
  python3 build_knowledge_base.py --dry-run
  python3 build_knowledge_base.py --collection "我的知识库"
  python3 build_knowledge_base.py --reset     # 删除旧 collection 重建

配置全部走环境变量，脚本内不硬编码绝对路径。
"""

import argparse
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    # 给出可执行的补救指令，而不是一串 traceback
    print("缺少依赖 requests，请先执行：pip install -r requirements.txt",
          file=sys.stderr)
    raise SystemExit(1)

TZ = timezone(timedelta(hours=8))

# ==================== 配置 ====================

# 安装了 open-webui 的虚拟环境（用于 import 它的 create_token 签发 JWT）
# 留空则使用当前解释器的 site-packages
VENV_DIR = Path(os.environ.get("WEBUI_VENV", "")) if os.environ.get("WEBUI_VENV") else None

# Open WebUI 数据目录与密钥
DATA_DIR = Path(os.environ.get("WEBUI_DATA_DIR", str(Path.home() / "webui-data")))
SECRET_FILE = Path(os.environ.get("WEBUI_SECRET_FILE", str(DATA_DIR / ".webui_secret_key")))

WEBUI_BASE = os.environ.get("WEBUI_BASE", "http://localhost:8080")
ADMIN_USER_ID = os.environ.get("WEBUI_ADMIN_ID", "")
ADMIN_EMAIL = os.environ.get("WEBUI_ADMIN_EMAIL", "")
ADMIN_NAME = os.environ.get("WEBUI_ADMIN_NAME", "admin")

# 笔记库根目录
VAULT = Path(os.environ.get("VAULT_DIR", str(Path.home() / "notes")))

# 纳入检索的目录（历史归档区故意排除：内容陈旧、信噪比低，
# 进库会把有用结果的排名挤下去）
SOURCE_DIRS = [
    VAULT / "00-长期记忆",
    VAULT / "20-通用知识",
    VAULT / "30-知识资产",
    VAULT / "40-技能库",
]

STATE_FILE = VAULT / f".{Path(__file__).stem}_state.json"

DEFAULT_COLLECTION = "本地笔记知识库"
DEFAULT_DESCRIPTION = (
    "从本地 Markdown 笔记库自动同步：长期记忆 + 通用知识 + "
    "知识资产 + 技能库（SOP / 模板 / 检查清单）"
)

MAX_RETRIES = 5
BASE_DELAY = 2.0


def log(*args):
    print(*args, file=sys.stderr)


def make_jwt():
    """签发一个 30 分钟有效的 admin JWT。

    直接用 Open WebUI 自己的 create_token 实现，避免手拼 payload 出错。
    """
    if VENV_DIR and VENV_DIR.exists():
        for sp in VENV_DIR.glob("lib/python*/site-packages"):
            sys.path.insert(0, str(sp))

    os.environ["WEBUI_SECRET_KEY"] = SECRET_FILE.read_text().strip()
    os.environ["DATA_DIR"] = str(DATA_DIR)

    from open_webui.utils.auth import create_token

    return create_token(
        {
            "id": ADMIN_USER_ID,
            "email": ADMIN_EMAIL,
            "role": "admin",
            "name": ADMIN_NAME,
        },
        expires_delta=timedelta(minutes=30),
    )


class WebUIClient:
    """轻量客户端：每次请求独立连接，避免文件句柄堆积。"""

    def __init__(self, token: str):
        self.token = token
        self.base = WEBUI_BASE

    def _req(self, method, path, **kw):
        headers = {"Authorization": f"Bearer {self.token}", "Connection": "close"}
        if "headers" in kw:
            headers.update(kw.pop("headers"))
        url = f"{self.base}{path}"
        r = requests.request(method, url, headers=headers, **kw)
        if r.status_code >= 400:
            log(f"! {method.upper()} {path} -> {r.status_code}: {r.text[:300]}")
        r.raise_for_status()
        return r

    def get(self, path, **kw):
        return self._req("get", path, **kw).json()

    def post(self, path, **kw):
        return self._req("post", path, **kw).json()

    def delete(self, path, **kw):
        return self._req("delete", path, **kw).json()


def with_retry(fn, label=""):
    """指数退避重试：网络抖动 / 服务瞬时不可用时不让整批任务崩掉。"""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                log(f"  重试 {label} ({attempt + 1}/{MAX_RETRIES})，{delay:.1f}s 后: {e}")
                time.sleep(delay)
    raise last_err


def load_state():
    state = {"collection_id": None, "files": {}, "pending_add": [], "last_sync": None}
    if STATE_FILE.exists():
        try:
            old = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for k in state:
                if isinstance(old.get(k), type(state[k])):
                    state[k] = old[k]
            state["last_sync"] = old.get("last_sync")
        except Exception as e:
            log(f"! 状态文件解析失败: {e}")
    return state


def save_state(state):
    """原子写：先写临时文件再 replace，避免中途被杀留下半个 JSON。"""
    state["last_sync"] = datetime.now(TZ).isoformat()
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def scan_source_files():
    files = []
    for d in SOURCE_DIRS:
        if not d.exists():
            continue
        for md in sorted(d.rglob("*.md")):
            # 跳过索引页与空白模板，它们不承载知识内容
            if "MOC" in md.name or md.name.startswith("模板-"):
                continue
            rel = md.relative_to(VAULT)
            files.append({"path": md, "rel": str(rel), "name": md.name})
    return files


def find_collection(client, name):
    page = 1
    while True:
        data = client.get(f"/api/v1/knowledge/?page={page}")
        for item in data.get("items", []):
            if item.get("name") == name:
                return item.get("id")
        if not data.get("items"):
            break
        page += 1
        if page > 20:
            break
    return None


def create_collection(client, name, description):
    payload = {
        "name": name,
        "description": description,
        "access_grants": [],
        "data": {},
    }
    res = client.post("/api/v1/knowledge/create", json=payload)
    return res.get("id")


def upload_file_bg(client, path: Path):
    """上传文件并后台处理，返回 file_id。

    process_in_background 是关键：同步 embedding 在批量场景下会把
    服务端的处理队列打爆，同时客户端这边也会堆满等待的连接。
    """
    with open(path, "rb") as f:
        files = {"file": (path.name, f, "text/markdown")}
        data = {"metadata": json.dumps({"source": "notes-sync"}, ensure_ascii=False)}
        res = client.post(
            "/api/v1/files/?process=true&process_in_background=true",
            files=files,
            data=data,
        )
    return res.get("id")


def wait_file_ready(client, file_id, timeout=120):
    """轮询文件处理状态，直到 completed / failed / 超时。"""
    start = time.time()
    while time.time() - start < timeout:
        data = client.get(f"/api/v1/files/{file_id}/process/status")
        status = data.get("status")
        if status == "completed":
            return True
        if status == "failed":
            raise RuntimeError(f"file {file_id} processing failed: {data.get('error')}")
        time.sleep(0.8)
    raise TimeoutError(f"file {file_id} processing timeout")


def add_file_to_collection(client, collection_id, file_id):
    client.post(
        f"/api/v1/knowledge/{collection_id}/file/add",
        json={"file_id": file_id},
    )


def file_hash(path: Path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description="本地笔记库 → Open WebUI Knowledge")
    parser.add_argument("--dry-run", action="store_true", help="只看不写")
    parser.add_argument("--collection", default=DEFAULT_COLLECTION, help="Collection 名称")
    parser.add_argument("--description", default=DEFAULT_DESCRIPTION)
    parser.add_argument("--reset", action="store_true", help="删除同名旧 collection 重建")
    args = parser.parse_args()

    if not SECRET_FILE.exists():
        log(f"! 找不到 WebUI secret: {SECRET_FILE}")
        sys.exit(1)

    token = make_jwt()
    client = WebUIClient(token)

    try:
        ver = client.get("/api/version")
        log(f"Open WebUI 版本: {ver.get('version')}")
    except Exception as e:
        log(f"! 无法连接 Open WebUI: {e}")
        sys.exit(1)

    state = load_state()

    # 处理上一次「上传成功但加入 collection 失败」的遗留项 —— 断点续传
    if state.get("pending_add") and not args.dry_run and not args.reset:
        log(f"处理 {len(state['pending_add'])} 个待加入 collection 的文件")
        still_pending = []
        for entry in state["pending_add"]:
            rel, file_id = entry["rel"], entry["file_id"]
            try:
                wait_file_ready(client, file_id)
                add_file_to_collection(client, state["collection_id"], file_id)
                log(f"  补加入: {rel}")
                state["files"][rel] = {
                    "file_id": file_id, "hash": "",
                    "updated_at": datetime.now(TZ).isoformat(),
                }
            except Exception as e:
                log(f"! 补加入失败: {rel} -> {e}")
                still_pending.append(entry)
        state["pending_add"] = still_pending
        save_state(state)

    # 查找或创建 collection
    collection_id = None
    if not args.reset:
        collection_id = find_collection(client, args.collection)

    if args.reset and collection_id:
        if not args.dry_run:
            log(f"删除旧 collection: {args.collection} ({collection_id})")
            client.delete(f"/api/v1/knowledge/{collection_id}/delete")
        collection_id = None
        state["collection_id"] = None
        state["files"] = {}
        state["pending_add"] = []

    if not collection_id:
        if args.dry_run:
            log(f"[dry-run] 将创建 collection: {args.collection}")
        else:
            collection_id = create_collection(client, args.collection, args.description)
            state["collection_id"] = collection_id
            log(f"创建 collection: {args.collection} ({collection_id})")
    else:
        log(f"使用已有 collection: {args.collection} ({collection_id})")
        state["collection_id"] = collection_id

    files = scan_source_files()
    log(f"扫描到 {len(files)} 个 md 文件")

    stats = {"uploaded": 0, "added": 0, "skipped": 0, "failed": 0}

    for item in files:
        rel = item["rel"]
        path = item["path"]
        h = file_hash(path)

        # 已同步且内容未变 → 跳过（增量同步的核心）
        if rel in state["files"] and state["files"][rel].get("hash") == h:
            stats["skipped"] += 1
            continue

        if args.dry_run:
            log(f"[dry-run] 将上传: {rel}")
            continue

        try:
            # 内容变更重传前，先把旧文件从 collection 移除并删除本体，
            # 否则知识集里会随每次更新累积重复文件（旧向量也会污染检索）
            old = state["files"].get(rel) or {}
            old_fid = old.get("file_id")
            if old_fid and collection_id:
                try:
                    client.post(
                        f"/api/v1/knowledge/{collection_id}/file/remove",
                        json={"file_id": old_fid},
                    )
                    client.delete(f"/api/v1/files/{old_fid}")
                    log(f"  替换旧版本: {rel} ({old_fid[:8]}…)")
                except Exception as e:
                    log(f"! 移除旧版本失败(继续): {rel} -> {e}")

            file_id = with_retry(lambda: upload_file_bg(client, path), label=f"上传 {rel}")
            stats["uploaded"] += 1

            # 等待处理完成
            with_retry(lambda: wait_file_ready(client, file_id), label=f"处理 {rel}")

            # 加入 collection
            try:
                add_file_to_collection(client, collection_id, file_id)
                stats["added"] += 1
            except Exception as e:
                # 记录为待处理，下次重跑时补加
                log(f"! 加入 collection 失败，记录待补: {rel} -> {e}")
                state["pending_add"].append({"rel": rel, "file_id": file_id})

            state["files"][rel] = {
                "file_id": file_id,
                "hash": h,
                "updated_at": datetime.now(TZ).isoformat(),
            }
            save_state(state)
        except Exception as e:
            log(f"! 失败: {rel} -> {e}")
            stats["failed"] += 1

    if not args.dry_run:
        save_state(state)

    log(f"\n完成: 总计 {len(files)}, 上传 {stats['uploaded']}, "
        f"加入 {stats['added']}, 跳过 {stats['skipped']}, 失败 {stats['failed']}")
    if state["collection_id"]:
        log(f"Knowledge Collection: {WEBUI_BASE}/workspace/knowledge/{state['collection_id']}")


if __name__ == "__main__":
    main()
