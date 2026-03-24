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
        key = key.strip()
        value = value.strip().strip("\"'")
        os.environ.setdefault(key, value)


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
        if not part:
            continue
        result.add(int(part))
    return result


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OWNER_CHAT_ID = parse_int(os.getenv("OWNER_CHAT_ID"), 0)
OWNER_USER_ID = parse_int(os.getenv("OWNER_USER_ID"), 0)
ALLOWED_CHAT_IDS = parse_chat_id_set(os.getenv("ALLOWED_CHAT_IDS"))
AUTO_REPLY_ENABLED = parse_bool(os.getenv("AUTO_REPLY_ENABLED"), True)
RESPONSE_DELAY_SECONDS = parse_int(os.getenv("RESPONSE_DELAY_SECONDS"), 20)
TOPIC_COOLDOWN_SECONDS = parse_int(os.getenv("TOPIC_COOLDOWN_SECONDS"), 120)
LONG_POLL_TIMEOUT = parse_int(os.getenv("LONG_POLL_TIMEOUT"), 50)
HTTP_PORT = parse_int(os.getenv("PORT"), 8080)

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


@dataclass
class StyleConfig:
    question_triggers: list[str] = field(default_factory=list)
    risk_keywords: list[str] = field(default_factory=list)
    holding_reply_variants: list[str] = field(default_factory=list)
    fallback_reply_variants: list[str] = field(default_factory=list)
    owner_notification_title: str = "Николай"
    greeting_words: list[str] = field(default_factory=list)


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
    needs_attention: bool
    suppress_if_human_activity: bool = True


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
                item_id=entry["id"],
                keywords=[str(x).strip().lower() for x in entry.get("keywords", []) if str(x).strip()],
                response_variants=[str(x).strip() for x in entry.get("response_variants", []) if str(x).strip()],
                priority=int(entry.get("priority", 0)),
            )
        )
    return items



def load_style() -> StyleConfig:
    raw = read_json_file(STYLE_PATH, {})
    return StyleConfig(
        question_triggers=[str(x).strip().lower() for x in raw.get("question_triggers", []) if str(x).strip()],
        risk_keywords=[str(x).strip().lower() for x in raw.get("risk_keywords", []) if str(x).strip()],
        holding_reply_variants=[str(x).strip() for x in raw.get("holding_reply_variants", []) if str(x).strip()],
        fallback_reply_variants=[str(x).strip() for x in raw.get("fallback_reply_variants", []) if str(x).strip()],
        owner_notification_title=str(raw.get("owner_notification_title", "Николай")).strip() or "Николай",
        greeting_words=[str(x).strip().lower() for x in raw.get("greeting_words", []) if str(x).strip()],
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
    text = re.sub(r"[^a-zа-я0-9? ]+", " ", text)
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



def track_human_activity(chat_id: int, thread_id: int | None, user_id: int, message_id: int, timestamp: float) -> None:
    scope = chat_scope(chat_id, thread_id)
    with state.lock:
        state.scope_activity[scope].append((timestamp, user_id, message_id))



def has_other_human_activity(candidate: PendingCandidate) -> bool:
    scope = chat_scope(candidate.chat_id, candidate.thread_id)
    with state.lock:
        events = list(state.scope_activity.get(scope, []))
    for ts, user_id, _message_id in events:
        if ts > candidate.created_at and user_id != candidate.sender_id:
            return True
    return False



def score_faq_item(text: str, item: FAQItem) -> int:
    score = 0
    for keyword in item.keywords:
        if keyword and keyword in text:
            score += 3 if " " in keyword else 2
    return score + item.priority



def fill_template(text: str, sender_name: str) -> str:
    first_name = sender_name.split()[0] if sender_name else ""
    return text.replace("{client_name}", first_name)



def find_best_faq_answer(text: str, sender_name: str) -> tuple[str | None, str | None]:
    best_score = 0
    best_item: FAQItem | None = None
    with state.lock:
        faq_items = list(state.faq_items)
    for item in faq_items:
        score = score_faq_item(text, item)
        if score > best_score and item.response_variants:
            best_score = score
            best_item = item
    if not best_item or best_score < 2:
        return None, None
    answer = fill_template(choose(best_item.response_variants), sender_name)
    return answer, best_item.item_id



def looks_like_question(text: str) -> bool:
    if not text:
        return False
    clean = clean_for_match(text)
    if "?" in text:
        return True
    with state.lock:
        triggers = list(state.style.question_triggers)
        greetings = list(state.style.greeting_words)
    return any(trigger in clean for trigger in triggers + greetings)



def has_risk_keywords(text: str) -> list[str]:
    clean = clean_for_match(text)
    with state.lock:
        risk_keywords = list(state.style.risk_keywords)
    hits = [kw for kw in risk_keywords if kw in clean]
    return hits



def build_holding_reply(sender_name: str) -> str:
    with state.lock:
        variants = list(state.style.holding_reply_variants)
    return fill_template(choose(variants, "Вижу ваш вопрос. Передаю его Николаю на уточнение."), sender_name)



def build_fallback_reply(sender_name: str) -> str:
    with state.lock:
        variants = list(state.style.fallback_reply_variants)
    return fill_template(choose(variants, "Вижу сообщение. Чтобы не ошибиться, передаю вопрос Николаю."), sender_name)



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
        f"Клиент: {candidate.sender_name}\n"
        f"Причина: {candidate.reason}\n\n"
        f"Вопрос:\n{candidate.source_text}\n\n"
        f"Ответ:\n{candidate.response_text}"
    )



def short_notification(candidate: PendingCandidate, action: str) -> str:
    with state.lock:
        title = state.style.owner_notification_title
    scope_label = f"тема {candidate.thread_id}" if candidate.thread_id else "без темы"
    return f"{title}: {action}. {candidate.chat_title}, {scope_label}, {candidate.sender_name}. Причина: {candidate.reason}."



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
        send_message(
            chat_id,
            "Николай на связи. Команды: /status, /pause, /resume, /reload, /test.",
        )
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
                f"Разрешенные чаты: {', '.join(str(x) for x in sorted(ALLOWED_CHAT_IDS)) if ALLOWED_CHAT_IDS else 'все'}"
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
        send_message(
            chat_id,
            "Команды: /status, /pause, /resume, /reload, /test.",
        )



def build_candidate(message: dict[str, Any]) -> PendingCandidate | None:
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
    if not looks_like_question(source_text):
        return None

    risk_hits = has_risk_keywords(source_text)
    if risk_hits:
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
            response_text=build_holding_reply(sender_name),
            reason=f"риск: {', '.join(risk_hits[:3])}",
            needs_attention=True,
        )

    answer, faq_id = find_best_faq_answer(clean_for_match(source_text), sender_name)
    if answer:
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
            response_text=answer,
            reason=f"FAQ: {faq_id}",
            needs_attention=False,
        )

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
        response_text=build_fallback_reply(sender_name),
        reason="неизвестный вопрос",
        needs_attention=True,
    )



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

    if is_bot:
        return

    track_human_activity(chat_id, thread_id, user_id, message_id, timestamp)

    logger.info("Incoming message | chat=%s | thread=%s | user=%s | text=%s", chat_id, thread_id, user_id, extract_text(message)[:200])

    if chat_type == "private":
        process_owner_command(message)
        return

    if not allowed_chat(chat_id):
        return

    with state.lock:
        auto_mode = state.auto_reply_enabled
    if not auto_mode:
        return

    candidate = build_candidate(message)
    if not candidate:
        return

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
            send_message(OWNER_CHAT_ID, short_notification(candidate, "пропуск по cooldown"))
            continue

        if candidate.suppress_if_human_activity and has_other_human_activity(candidate):
            logger.info("Skipped due to human activity for chat=%s message=%s", candidate.chat_id, candidate.message_id)
            send_message(OWNER_CHAT_ID, short_notification(candidate, "пропуск: уже ответили в теме"))
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
            action = "ответил" if not candidate.needs_attention else "ответил и просит внимания"
            send_message(OWNER_CHAT_ID, owner_notification(candidate, action))
        except Exception as exc:
            logger.exception("Failed to send candidate: %s", exc)
            send_message(OWNER_CHAT_ID, short_notification(candidate, f"ошибка отправки: {exc}"))



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
