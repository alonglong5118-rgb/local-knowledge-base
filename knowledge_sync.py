#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日知识闭环同步（WebUI 对话 → 本地笔记库 → WebUI 知识库）

流水线（一次运行完成三步）：
  ① 对话摘要：读 Open WebUI 的 SQLite 库里今天(00:00 起)的对话，
     调本机 Ollama 逐会话生成要点摘要；
     Ollama 不可用时自动降级为「抽取式」摘要（用户问题 + 关键句）。
  ② 写入笔记库：摘要写到 <VAULT>/30-知识资产/对话精华/YYYY-MM-DD.md
     （同日重跑覆盖更新）。
  ③ 回流知识库：调用 build_knowledge_base.py，把笔记库增量（含本次摘要）
     同步到 Open WebUI Knowledge Collection，供对话直接引用。

用法：
  python3 knowledge_sync.py                    # 全流程
  python3 knowledge_sync.py --dry-run          # 只生成摘要文件，不跑第③步
  python3 knowledge_sync.py --date 2026-01-15  # 补某一天

设计约束：仅用标准库（sqlite3 / urllib），可用系统自带任何 python3 运行，
使定时任务不依赖虚拟环境。
"""

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ==================== 配置 ====================
# 全部支持环境变量覆盖，脚本内不硬编码绝对路径。

TZ = timezone(timedelta(hours=8))

# Open WebUI 的 SQLite 数据库
WEBUI_DB = Path(os.environ.get("WEBUI_DB", str(Path.home() / "webui-data" / "webui.db")))

# 笔记库根目录（Markdown 文件所在处）
VAULT = Path(os.environ.get("VAULT_DIR", str(Path.home() / "notes")))

# 摘要写入目录（相对 VAULT）
DIGEST_SUBDIR = "30-知识资产/对话精华"

# 负责「回流知识库」的脚本
BUILD_SCRIPT = VAULT / "build_knowledge_base.py"

# 运行 BUILD_SCRIPT 的解释器（该脚本需要 requests）
VENV_PY = os.environ.get("VENV_PY", sys.executable)

# 状态留痕文件
STATE_FILE = VAULT / f".{Path(__file__).stem}_state.json"

# 本地大模型服务
OLLAMA_BASE = os.environ.get("OLLAMA_BASE", "http://127.0.0.1:11434")
SUMMARY_MODELS = ["gemma3:4b", "qwen3:8b"]  # 依次尝试，取第一个可用的

MAX_SESSIONS = 10        # 单日最多精细摘要的会话数（控制单次任务规模）
TRANSCRIPT_CAP = 6000    # 送入模型的单会话文本上限（字符）
MSG_CAP = 900            # 单条消息截断（字符）
OLLAMA_TIMEOUT = 75      # 单会话摘要超时（秒）

# 主题分类规则：把每个会话按关键词归入某个主题桶。
# 按自己的知识库结构改写；命中多个时以字典顺序为准，都不命中归 FALLBACK_TOPIC。
TOPIC_RULES = {
    "笔记与知识管理": ["obsidian", "知识库", "笔记", "同步", "归档", "moc", "双链", "标签"],
    "AI 与本地模型": ["ollama", "llm", "rag", "embedding", "向量", "模型", "提示词"],
    "自动化与脚本": ["launchd", "cron", "定时", "脚本", "python", "自动化", "api"],
    "写作与内容": ["选题", "脚本", "文案", "标题", "素材", "初稿"],
}
FALLBACK_TOPIC = "通用对话"


def log(*a):
    print(*a, file=sys.stderr)


def classify(text):
    """按关键词把会话归入主题桶。"""
    blob = (text or "").lower()
    for name, kws in TOPIC_RULES.items():
        if any(kw in blob for kw in kws):
            return name
    return FALLBACK_TOPIC


def strip_think(t):
    """去掉模型输出里的推理段（部分模型会带 <think>...</think>）。"""
    return re.sub(r"<think>.*?</think>", "", t or "", flags=re.S).strip()


# ---------- ① 读今天的对话 ----------

def extract_text(content):
    """把 Open WebUI 里各种形态的 message content 归一成纯文本。"""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict):
                if p.get("type") == "text":
                    parts.append(p.get("text", ""))
                elif p.get("type") == "output_text":
                    parts.append(p.get("text", ""))
                elif p.get("type") in ("image_url", "image"):
                    parts.append("[图片]")
            elif isinstance(p, str):
                parts.append(p)
        return "\n".join(parts)
    if isinstance(content, dict):
        for k in ("content", "text", "message"):
            if k in content:
                return extract_text(content[k])
    return str(content)


def fetch_today_chats(date_str):
    """返回 [{id, title, model, updated, msgs:[(role, text)]}]，按更新时间倒序。

    注意：以只读方式打开源库（mode=ro），避免与 Open WebUI 自身的写入互相锁库。
    """
    since = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=TZ)
    since_ts = since.timestamp()
    db = sqlite3.connect(f"file:{WEBUI_DB}?mode=ro", uri=True)
    try:
        cur = db.cursor()
        cur.execute("SELECT id, title, updated_at FROM chat ORDER BY updated_at DESC")
        sessions = []
        for cid, title, updated_at in cur.fetchall():
            # 兼容秒 / 毫秒两种时间戳
            if updated_at and updated_at > 1e12:
                updated_at = updated_at / 1000
            updated_dt = datetime.fromtimestamp(updated_at or 0, tz=timezone.utc).astimezone(TZ)
            if updated_dt.timestamp() < since_ts:
                continue
            cur2 = db.cursor()
            cur2.execute("""
                SELECT role, content, output, model_id
                FROM chat_message WHERE chat_id = ?
                ORDER BY created_at ASC, id ASC
            """, (cid,))
            msgs = []
            model = ""
            for role, content_raw, output_raw, model_id in cur2.fetchall():
                if role not in ("user", "assistant"):
                    continue
                text = extract_text(json.loads(content_raw) if content_raw else None)
                if role == "assistant":
                    text = strip_think(text)
                    model = model_id or model
                text = (text or "").strip()
                if not text:
                    continue
                msgs.append((role, text))
            if msgs:
                sessions.append({
                    "id": cid,
                    "title": re.sub(r'[\\/:*?"<>|\n]', " ", title or "未命名").strip() or "未命名",
                    "model": model or "未知模型",
                    "updated": updated_dt.strftime("%H:%M"),
                    "msgs": msgs,
                })
        return sessions
    finally:
        db.close()


# ---------- Ollama 摘要 ----------

def ollama_models():
    try:
        with urllib.request.urlopen(OLLAMA_BASE + "/api/tags", timeout=4) as r:
            return [m["name"] for m in json.loads(r.read()).get("models", [])]
    except Exception:
        return []


def ollama_summarize(model, transcript):
    prompt = (
        "以下是一段用户与AI助手的对话。请用中文把它总结为 3-5 个要点，"
        "每行一个、以'- '开头；聚焦：用户问了什么、得出什么结论、做了什么决定。"
        "不要评论、不要扩展、不要客套。\n\n=== 对话 ===\n" + transcript
    )
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        "options": {"temperature": 0.3},
    }).encode("utf-8")
    req = urllib.request.Request(
        OLLAMA_BASE + "/api/chat", data=body,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT) as r:
        data = json.loads(r.read())
    return strip_think(data.get("message", {}).get("content", "")).strip()


def fallback_summary(msgs):
    """Ollama 不可用时的降级方案：抽取式摘要（取每段首句）。

    质量低于模型摘要，但保证任务不中断 —— 这是三级降级的第 2 级。
    """
    lines = []
    for role, text in msgs:
        first = re.split(r"[。\n]", text)[0].strip()
        if not first:
            continue
        who = "用户问" if role == "user" else "要点"
        lines.append(f"- {who}：{first[:120]}")
        if len(lines) >= 6:
            break
    return "\n".join(lines) if lines else "-（无可提取内容）"


def build_transcript(msgs):
    """把会话拼成送入模型的文本，带总长上限（防止超长对话把任务拖死）。"""
    buf = []
    total = 0
    for role, text in msgs:
        who = "用户" if role == "user" else "助手"
        seg = f"{who}: {text[:MSG_CAP]}"
        buf.append(seg)
        total += len(seg)
        if total > TRANSCRIPT_CAP:
            buf.append("…（后续内容截断）")
            break
    return "\n\n".join(buf)


# ---------- ② 摘要写笔记库 ----------

def write_digest(date_str, entries, used_llm):
    digest_dir = VAULT / DIGEST_SUBDIR
    digest_dir.mkdir(parents=True, exist_ok=True)
    path = digest_dir / f"{date_str}.md"
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    out = [
        "---",
        'source: openwebui-digest',
        f'date: "{date_str}"',
        f'sessions: {len(entries)}',
        f'summary_engine: "{ "ollama" if used_llm else "extractive" }"',
        'tags:',
        '  - OpenWebUI',
        '  - 对话精华',
        "---",
        "",
        f"# 对话精华 · {date_str}",
        "",
        f"> 由每日知识闭环自动生成 · {len(entries)} 个会话 · "
        f"摘要引擎：{'本机 Ollama' if used_llm else '抽取式（Ollama 不可用）'} · 生成于 {now}",
        "",
    ]
    for e in entries:
        out.append(f"## {e['title']}")
        out.append("")
        out.append(f"- 时间：{e['updated']}　|　模型：{e['model']}　|　主题：{e['project']}")
        out.append("")
        out.append(e["summary"])
        out.append("")
    path.write_text("\n".join(out), encoding="utf-8")
    return path


# ---------- ③ 回流知识库 ----------

def run_build():
    """调用 build_knowledge_base.py。

    任何一环不满足（库不存在 / WebUI 没起 / 解释器缺失）都返回 skip 而不是报错，
    因为这三件事都不影响已生成的摘要，且下次运行会续上。
    """
    if not WEBUI_DB.exists():
        return "skip", "webui.db 不存在"
    try:
        urllib.request.urlopen("http://localhost:8080/health", timeout=4)
    except Exception:
        try:
            urllib.request.urlopen("http://localhost:8080/", timeout=4)
        except Exception:
            return "skip", "Open WebUI 未运行（本步骤跳过，下次运行会续传）"
    if not Path(VENV_PY).exists():
        return "skip", f"解释器不存在: {VENV_PY}"
    r = subprocess.run(
        [VENV_PY, str(BUILD_SCRIPT)],
        cwd=str(BUILD_SCRIPT.parent),
        capture_output=True, text=True, timeout=1200,
    )
    tail = "\n".join((r.stderr or r.stdout or "").strip().splitlines()[-6:])
    return ("ok" if r.returncode == 0 else "fail"), tail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="补某个日期 YYYY-MM-DD（默认今天）")
    ap.add_argument("--dry-run", action="store_true", help="只生成摘要，不跑回流")
    args = ap.parse_args()

    date_str = args.date or datetime.now(TZ).strftime("%Y-%m-%d")
    log(f"== 每日知识闭环 {date_str} ==")

    # ① 摘要
    sessions = fetch_today_chats(date_str)
    log(f"今日会话: {len(sessions)}")
    entries, used_llm = [], False
    if sessions:
        models = ollama_models()
        pick = next((m for m in SUMMARY_MODELS if m in models), None)
        log(f"Ollama 摘要模型: {pick or '不可用 → 抽取式'}")
        for s in sessions[:MAX_SESSIONS]:
            s["project"] = classify(s["title"] + " " + " ".join(t for _, t in s["msgs"])[:1500])
            if pick:
                try:
                    s["summary"] = ollama_summarize(pick, build_transcript(s["msgs"]))
                    used_llm = True
                    if not s["summary"]:
                        s["summary"] = fallback_summary(s["msgs"])
                except Exception as e:
                    log(f"! 摘要失败({s['title'][:20]}): {e} → 抽取式")
                    s["summary"] = fallback_summary(s["msgs"])
            else:
                s["summary"] = fallback_summary(s["msgs"])
            entries.append(s)
            log(f"  ✓ {s['title'][:30]}")
        if len(sessions) > MAX_SESSIONS:
            log(f"! 会话超过 {MAX_SESSIONS} 个，其余 {len(sessions)-MAX_SESSIONS} 个未摘要")

    # ② 写笔记库
    if entries:
        path = write_digest(date_str, entries, used_llm)
        log(f"摘要已写入: {path}")
    else:
        path = None
        log("今日无新对话，不生成摘要文件")

    # ③ 回流
    if args.dry_run:
        log("--dry-run：跳过知识库回流")
        build_status, build_info = "skip", "dry-run"
    else:
        build_status, build_info = run_build()
        log(f"回流状态: {build_status}")
        log(build_info)

    # 状态留痕（任何一天的运行状态都可回溯）
    STATE_FILE.write_text(json.dumps({
        "last_run": datetime.now(TZ).isoformat(),
        "date": date_str,
        "sessions": len(entries),
        "used_llm": used_llm,
        "digest": str(path) if path else None,
        "build_status": build_status,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    # 人类可读汇报（stdout，方便定时任务日志一眼看清结果）
    print(f"日期 {date_str} | 会话 {len(entries)} 个 | "
          f"摘要引擎 {'ollama' if used_llm else 'extractive'} | "
          f"摘要文件 {path.name if path else '无'} | "
          f"回流 {build_status}")
    return 0 if build_status in ("ok", "skip") else 1


if __name__ == "__main__":
    sys.exit(main())
