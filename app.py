from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"\''))


load_dotenv(BASE_DIR / ".env")

FAQ_PATH = Path(os.getenv("FAQ_PATH", DATA_DIR / "faq.json"))
STYLE_PATH = Path(os.getenv("STYLE_PATH", DATA_DIR / "style.json"))
STATE_PATH = Path(os.getenv("STATE_PATH", DATA_DIR / "state.json"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("nikolay-bot")


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_chat_id_set(value: str | None) -> set[int]:
    if not value:
        return set()
    result: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if part:
            result.add(int(part))
    return result


def parse_int_set(value: str | None) -> set[int]:
    if not value:
        return set()
    result: set[int] = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            logger.warning("Skipped invalid integer in env list: %s", part)
    return result


def parse_text_set(value: str | None) -> set[str]:
    if not value:
        return set()
    return {x.strip().lower().lstrip("@") for x in value.split(",") if x.strip()}


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OWNER_CHAT_ID = parse_int(os.getenv("OWNER_CHAT_ID"), 0)
OWNER_USER_ID = parse_int(os.getenv("OWNER_USER_ID"), 0)
ALLOWED_CHAT_IDS = parse_chat_id_set(os.getenv("ALLOWED_CHAT_IDS"))
AUTO_REPLY_ENABLED = parse_bool(os.getenv("AUTO_REPLY_ENABLED"), True)
RESPONSE_DELAY_SECONDS = parse_int(os.getenv("RESPONSE_DELAY_SECONDS"), 12)
TOPIC_COOLDOWN_SECONDS = parse_int(os.getenv("TOPIC_COOLDOWN_SECONDS"), 90)
LONG_POLL_TIMEOUT = parse_int(os.getenv("LONG_POLL_TIMEOUT"), 50)
HTTP_PORT = parse_int(os.getenv("PORT"), 8080)
MANAGER_USER_IDS = parse_int_set(os.getenv("MANAGER_USER_IDS", "334161944"))
MANAGER_USERNAMES = parse_text_set(os.getenv("MANAGER_USERNAMES", "denislenivko"))

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is required")
if OWNER_CHAT_ID == 0:
    raise RuntimeError("OWNER_CHAT_ID is required")

API_BASE = f"https://api.telegram.org/bot{TOKEN}"


@dataclass
class FAQItem:
    item_id: str
    keywords: list[str]
    response_variants: list[str]
    priority: int = 0
    manager_only: bool = False
    requires_link: bool = False
    requires_deadline: bool = False


@dataclass
class StyleConfig:
    owner_notification_title: str = "Николай"
    manager_task_triggers: list[str] = field(default_factory=list)
    manager_deadline_triggers: list[str] = field(default_factory=list)
    manager_link_triggers: list[str] = field(default_factory=list)
    manager_status_triggers: list[str] = field(default_factory=list)
    manager_ack_variants: list[str] = field(default_factory=list)
    manager_link_ack_variants: list[str] = field(default_factory=list)
    manager_deadline_ack_variants: list[str] = field(default_factory=list)
    manager_status_ack_variants: list[str] = field(default_factory=list)
    joke_command_aliases: list[str] = field(default_factory=list)
    joke_variants: list[str] = field(default_factory=list)


@dataclass
class PendingCandidate:
    due_at: float
    created_at: float
    chat_id: int
    chat_title: str
    thread_id: int | None
    message_id: int
    sender_id: int
    sender_name: str
    source_text: str
    response_text: str
    reason: str
    suppress_if_human_activity: bool = False
    notify_owner: bool = True


@dataclass
class RuntimeState:
    faq_items: list[FAQItem] = field(default_factory=list)
    style: StyleConfig = field(default_factory=StyleConfig)
    update_offset: int = 0
    auto_reply_enabled: bool = AUTO_REPLY_ENABLED
    pending: dict[tuple[int, int], PendingCandidate] = field(default_factory=dict)
    scope_activity: dict[tuple[int, int], deque[tuple[float, int, int]]] = field(
        default_factory=lambda: defaultdict(lambda: deque(maxlen=50))
    )
    last_bot_message_at: dict[tuple[int, int], float] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)


state = RuntimeState()


def now_ts() -> float:
    return time.time()


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        logger.warning("JSON file does not exist: %s", path)
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_faq() -> list[FAQItem]:
    raw = read_json_file(FAQ_PATH, [])
    items: list[FAQItem] = []
    for entry in raw:
        items.append(
            FAQItem(
                item_id=str(entry["id"]),
                keywords=[str(x).strip().lower() for x in entry.get("keywords", []) if str(x).strip()],
                response_variants=[str(x).strip() for x in entry.get("response_variants", []) if str(x).strip()],
                priority=int(entry.get("priority", 0)),
                manager_only=bool(entry.get("manager_only", False)),
                requires_link=bool(entry.get("requires_link", False)),
                requires_deadline=bool(entry.get("requires_deadline", False)),
            )
        )
    return items


def load_style() -> StyleConfig:
    raw = read_json_file(STYLE_PATH, {})
    return StyleConfig(
        owner_notification_title=str(raw.get("owner_notification_title", "Николай")).strip() or "Николай",
        manager_task_triggers=[str(x).strip().lower() for x in raw.get("manager_task_triggers", []) if str(x).strip()],
        manager_deadline_triggers=[str(x).strip().lower() for x in raw.get("manager_deadline_triggers", []) if str(x).strip()],
        manager_link_triggers=[str(x).strip().lower() for x in raw.get("manager_link_triggers", []) if str(x).strip()],
        manager_status_triggers=[str(x).strip().lower() for x in raw.get("manager_status_triggers", []) if str(x).strip()],
        manager_ack_variants=[str(x).strip() for x in raw.get("manager_ack_variants", []) if str(x).strip()],
        manager_link_ack_variants=[str(x).strip() for x in raw.get("manager_link_ack_variants", []) if str(x).strip()],
        manager_deadline_ack_variants=[str(x).strip() for x in raw.get("manager_deadline_ack_variants", []) if str(x).strip()],
        manager_status_ack_variants=[str(x).strip() for x in raw.get("manager_status_ack_variants", []) if str(x).strip()],
        joke_command_aliases=[str(x).strip().lower() for x in raw.get("joke_command_aliases", []) if str(x).strip()],
        joke_variants=[str(x).strip() for x in raw.get("joke_variants", []) if str(x).strip()],
    )


def save_state() -> None:
    payload = {
        "update_offset": state.update_offset,
        "auto_reply_enabled": state.auto_reply_enabled,
        "saved_at": utc_now_str(),
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state() -> None:
    raw = read_json_file(STATE_PATH, {})
    state.update_offset = int(raw.get("update_offset", 0))
    state.auto_reply_enabled = bool(raw.get("auto_reply_enabled", AUTO_REPLY_ENABLED))


def reload_knowledge() -> None:
    with state.lock:
        state.faq_items = load_faq()
        state.style = load_style()
    logger.info("Knowledge loaded: %s FAQ items", len(state.faq_items))


def api_request(method: str, payload: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    response = requests.post(f"{API_BASE}/{method}", json=payload or {}, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error in {method}: {data}")
    return data["result"]


def send_message(chat_id: int, text: str, thread_id: int | None = None, reply_to_message_id: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True
    return api_request("sendMessage", payload)


def get_updates(offset: int) -> list[dict[str, Any]]:
    payload = {
        "offset": offset,
        "timeout": LONG_POLL_TIMEOUT,
        "allowed_updates": ["message", "edited_message"],
    }
    return api_request("getUpdates", payload, timeout=LONG_POLL_TIMEOUT + 10)


def normalize(text: str) -> str:
    text = text.lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_for_match(text: str) -> str:
    text = normalize(text)
    text = re.sub(r"[^a-zа-я0-9?/:._#@ -]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def choose(items: list[str], default: str = "") -> str:
    return random.choice(items) if items else default


def allowed_chat(chat_id: int) -> bool:
    return not ALLOWED_CHAT_IDS or chat_id in ALLOWED_CHAT_IDS


def extract_text(message: dict[str, Any]) -> str:
    return str(message.get("text") or message.get("caption") or "").strip()


def chat_scope(chat_id: int, thread_id: int | None) -> tuple[int, int]:
    return (chat_id, thread_id or 0)


def format_sender_name(user: dict[str, Any] | None) -> str:
    if not user:
        return "Неизвестный"
    parts = [user.get("first_name"), user.get("last_name")]
    full = " ".join(str(x).strip() for x in parts if x).strip()
    if full:
        return full
    if user.get("username"):
        return f"@{user['username']}"
    return f"user_{user.get('id', 'unknown')}"


def format_topic_label(message: dict[str, Any]) -> str:
    reply = message.get("reply_to_message") or {}
    if reply.get("forum_topic_created"):
        return str(reply["forum_topic_created"].get("name") or "Тема")
    return f"Тема {message.get('message_thread_id')}" if message.get("message_thread_id") else "Без темы"


def track_human_activity(chat_id: int, thread_id: int | None, user_id: int, message_id: int, timestamp: float) -> None:
    scope = chat_scope(chat_id, thread_id)
    with state.lock:
        state.scope_activity[scope].append((timestamp, user_id, message_id))


def score_faq_item(text: str, item: FAQItem, has_link: bool, has_deadline: bool) -> int:
    score = 0
    for keyword in item.keywords:
        if keyword and keyword in text:
            score += 4 if " " in keyword else 2
    if item.requires_link and has_link:
        score += 3
    if item.requires_deadline and has_deadline:
        score += 3
    return score + item.priority


def fill_template(text: str, sender_name: str) -> str:
    first_name = sender_name.split()[0] if sender_name else ""
    return text.replace("{sender_name}", sender_name).replace("{first_name}", first_name)


def is_manager_message(user: dict[str, Any]) -> bool:
    user_id = int(user.get("id", 0))
    username = str(user.get("username") or "").strip().lower().lstrip("@")
    return (user_id in MANAGER_USER_IDS) or (bool(username) and username in MANAGER_USERNAMES)


def manager_signal_hits(text: str) -> tuple[list[str], list[str], list[str], list[str]]:
    clean = clean_for_match(text)
    with state.lock:
        style = state.style
        task_hits = [kw for kw in style.manager_task_triggers if kw in clean]
        deadline_hits = [kw for kw in style.manager_deadline_triggers if kw in clean]
        link_hits = [kw for kw in style.manager_link_triggers if kw in clean]
        status_hits = [kw for kw in style.manager_status_triggers if kw in clean]
    return task_hits, deadline_hits, link_hits, status_hits


def looks_like_manager_task(text: str) -> bool:
    task_hits, deadline_hits, link_hits, status_hits = manager_signal_hits(text)
    return any([task_hits, deadline_hits, link_hits, status_hits])


def find_best_manager_answer(text: str, sender_name: str) -> tuple[str | None, str | None]:
    clean = clean_for_match(text)
    has_link = any(x in clean for x in ["https://", "http://", "docs.google.com", "spreadsheets", "drive.google.com"])
    has_deadline = bool(re.search(r"\bдо\s+\d{1,2}[:.]?\d{0,2}\b", clean)) or any(x in clean for x in ["дедлайн", "сегодня", "завтра", "asap", "срочно"])

    best_score = 0
    best_item: FAQItem | None = None
    with state.lock:
        faq_items = [item for item in state.faq_items if item.manager_only]
    for item in faq_items:
        score = score_faq_item(clean, item, has_link, has_deadline)
        if score > best_score and item.response_variants:
            best_score = score
            best_item = item

    if best_item and best_score >= 3:
        return fill_template(choose(best_item.response_variants), sender_name), best_item.item_id

    with state.lock:
        style = state.style
    if has_deadline and style.manager_deadline_ack_variants:
        return fill_template(choose(style.manager_deadline_ack_variants), sender_name), "deadline_ack"
    if has_link and style.manager_link_ack_variants:
        return fill_template(choose(style.manager_link_ack_variants), sender_name), "link_ack"
    task_hits, _deadline_hits, _link_hits, status_hits = manager_signal_hits(clean)
    if status_hits and style.manager_status_ack_variants:
        return fill_template(choose(style.manager_status_ack_variants), sender_name), "status_ack"
    if task_hits and style.manager_ack_variants:
        return fill_template(choose(style.manager_ack_variants), sender_name), "task_ack"
    return None, None


def joke_requested(text: str) -> bool:
    clean = clean_for_match(text)
    if clean.startswith("/anekdot") or clean.startswith("/анекдот"):
        return True
    with state.lock:
        aliases = list(state.style.joke_command_aliases)
    return clean in aliases


def queue_candidate(candidate: PendingCandidate) -> None:
    key = (candidate.chat_id, candidate.message_id)
    with state.lock:
        state.pending[key] = candidate
    logger.info("Queued candidate for chat=%s message=%s reason=%s", candidate.chat_id, candidate.message_id, candidate.reason)


def owner_notification(candidate: PendingCandidate, action: str) -> str:
    with state.lock:
        title = state.style.owner_notification_title
    scope_label = f"Тема {candidate.thread_id}" if candidate.thread_id else "Без темы"
    return (
        f"{title}: {action}\n"
        f"Чат: {candidate.chat_title} ({candidate.chat_id})\n"
        f"Контекст: {scope_label}\n"
        f"Автор: {candidate.sender_name}\n"
        f"Причина: {candidate.reason}\n\n"
        f"Сообщение:\n{candidate.source_text}\n\n"
        f"Ответ:\n{candidate.response_text}"
    )


def process_owner_command(message: dict[str, Any]) -> None:
    global OWNER_CHAT_ID

    text = extract_text(message)
    user = message.get("from") or {}
    user_id = int(user.get("id", 0))
    chat_id = int(message["chat"]["id"])

    if OWNER_USER_ID and user_id != OWNER_USER_ID:
        send_message(chat_id, "У тебя нет прав на управление этим ботом.")
        return

    if text.startswith("/start"):
        OWNER_CHAT_ID = chat_id
        send_message(chat_id, "Николай на связи. Команды: /status, /pause, /resume, /reload, /test.")
        return
    if text.startswith("/status"):
        with state.lock:
            pending_count = len(state.pending)
            faq_count = len(state.faq_items)
            auto_mode = state.auto_reply_enabled
        send_message(
            chat_id,
            (
                f"Статус: {'включен' if auto_mode else 'пауза'}\n"
                f"FAQ: {faq_count}\n"
                f"В очереди: {pending_count}\n"
                f"Чаты: {', '.join(str(x) for x in sorted(ALLOWED_CHAT_IDS)) if ALLOWED_CHAT_IDS else 'все'}\n"
                f"Руководитель: ids={sorted(MANAGER_USER_IDS)} usernames={sorted(MANAGER_USERNAMES)}"
            ),
        )
        return
    if text.startswith("/pause"):
        with state.lock:
            state.auto_reply_enabled = False
        save_state()
        send_message(chat_id, "Автоответы поставлены на паузу.")
        return
    if text.startswith("/resume"):
        with state.lock:
            state.auto_reply_enabled = True
        save_state()
        send_message(chat_id, "Автоответы снова включены.")
        return
    if text.startswith("/reload"):
        reload_knowledge()
        send_message(chat_id, "Файлы FAQ и стиля перечитаны.")
        return
    if text.startswith("/test"):
        send_message(chat_id, "Тестовое уведомление: я работаю.")
        return
    if text.startswith("/help"):
        send_message(chat_id, "Команды: /status, /pause, /resume, /reload, /test.")


def build_manager_candidate(message: dict[str, Any]) -> PendingCandidate | None:
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    chat_id = int(chat.get("id", 0))
    thread_id = message.get("message_thread_id")
    message_id = int(message.get("message_id", 0))
    sender_id = int(user.get("id", 0))
    sender_name = format_sender_name(user)
    chat_title = str(chat.get("title") or chat.get("username") or chat_id)
    source_text = extract_text(message)

    if not source_text:
        return None
    if not is_manager_message(user):
        return None
    if not looks_like_manager_task(source_text):
        return None

    response_text, response_reason = find_best_manager_answer(source_text, sender_name)
    if not response_text:
        with state.lock:
            response_text = choose(state.style.manager_ack_variants, "Принял. Подхватил в работу.")
        response_reason = "generic_manager_ack"

    return PendingCandidate(
        due_at=now_ts() + RESPONSE_DELAY_SECONDS,
        created_at=float(message.get("date", now_ts())),
        chat_id=chat_id,
        chat_title=chat_title,
        thread_id=thread_id,
        message_id=message_id,
        sender_id=sender_id,
        sender_name=sender_name,
        source_text=source_text,
        response_text=response_text,
        reason=response_reason or "manager_task",
        suppress_if_human_activity=False,
        notify_owner=True,
    )


def send_joke(chat_id: int, thread_id: int | None, reply_to_message_id: int | None) -> None:
    with state.lock:
        joke = choose(state.style.joke_variants, "Коллеги, шутки закончились, осталась только продуктивность 😄")
    send_message(chat_id, joke, thread_id=thread_id, reply_to_message_id=reply_to_message_id)


def handle_message(message: dict[str, Any]) -> None:
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    if not chat or not user:
        return

    chat_id = int(chat.get("id", 0))
    user_id = int(user.get("id", 0))
    is_bot = bool(user.get("is_bot", False))
    thread_id = message.get("message_thread_id")
    timestamp = float(message.get("date", now_ts()))
    message_id = int(message.get("message_id", 0))
    chat_type = str(chat.get("type", ""))
    text = extract_text(message)

    if is_bot:
        return

    track_human_activity(chat_id, thread_id, user_id, message_id, timestamp)
    logger.info("Incoming | chat=%s | thread=%s | user=%s | text=%s", chat_id, thread_id, user_id, text[:250])

    if chat_type == "private":
        process_owner_command(message)
        return

    if not allowed_chat(chat_id):
        return

    if joke_requested(text):
        send_joke(chat_id, thread_id, message_id)
        return

    with state.lock:
        auto_mode = state.auto_reply_enabled
    if not auto_mode:
        return

    candidate = build_manager_candidate(message)
    if candidate:
        queue_candidate(candidate)


def flush_pending() -> None:
    ready: list[PendingCandidate] = []
    with state.lock:
        for key, candidate in list(state.pending.items()):
            if candidate.due_at <= now_ts():
                ready.append(candidate)
                del state.pending[key]

    for candidate in ready:
        scope = chat_scope(candidate.chat_id, candidate.thread_id)
        last_bot_ts = state.last_bot_message_at.get(scope, 0.0)
        if now_ts() - last_bot_ts < TOPIC_COOLDOWN_SECONDS:
            logger.info("Skipped due to cooldown for scope=%s", scope)
            continue
        try:
            send_message(
                candidate.chat_id,
                candidate.response_text,
                thread_id=candidate.thread_id,
                reply_to_message_id=candidate.message_id,
            )
            with state.lock:
                state.last_bot_message_at[scope] = now_ts()
            if candidate.notify_owner:
                send_message(OWNER_CHAT_ID, owner_notification(candidate, "ответил на поручение"))
        except Exception as exc:
            logger.exception("Failed to send candidate: %s", exc)
            send_message(OWNER_CHAT_ID, f"Николай: ошибка отправки ответа в чат {candidate.chat_title}: {exc}")


def poller_loop() -> None:
    logger.info("Poller loop started")
    while True:
        try:
            updates = get_updates(state.update_offset)
            for update in updates:
                update_id = int(update["update_id"])
                state.update_offset = max(state.update_offset, update_id + 1)
                save_state()
                message = update.get("message") or update.get("edited_message")
                if message:
                    handle_message(message)
            flush_pending()
        except requests.RequestException as exc:
            logger.warning("Network error: %s", exc)
            time.sleep(5)
        except Exception as exc:
            logger.exception("Unexpected error in poller loop: %s", exc)
            time.sleep(5)


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = {
            "status": "ok",
            "auto_reply_enabled": state.auto_reply_enabled,
            "faq_items": len(state.faq_items),
            "pending": len(state.pending),
            "manager_user_ids": sorted(MANAGER_USER_IDS),
            "manager_usernames": sorted(MANAGER_USERNAMES),
            "updated_at": utc_now_str(),
        }
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def serve_http() -> None:
    server = HTTPServer(("0.0.0.0", HTTP_PORT), HealthHandler)
    logger.info("Health server running on port %s", HTTP_PORT)
    server.serve_forever()


def main() -> None:
    load_state()
    reload_knowledge()
    threading.Thread(target=poller_loop, daemon=True).start()
    serve_http()


if __name__ == "__main__":
    main()
