from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATE_PATH = Path(os.getenv("STATE_PATH", DATA_DIR / "state.json"))


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


def parse_int_set(value: str | None) -> set[int]:
    result: set[int] = set()
    if not value:
        return result
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            logger.warning("Ignored invalid int value: %s", part)
    return result


def parse_str_set(value: str | None) -> set[str]:
    if not value:
        return set()
    return {part.strip().lstrip("@").lower() for part in value.split(",") if part.strip()}


TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OWNER_CHAT_ID = parse_int(os.getenv("OWNER_CHAT_ID"), 0)
OWNER_USER_ID = parse_int(os.getenv("OWNER_USER_ID"), 0)
ALLOWED_CHAT_IDS = parse_int_set(os.getenv("ALLOWED_CHAT_IDS"))
AUTO_REPLY_ENABLED = parse_bool(os.getenv("AUTO_REPLY_ENABLED"), True)
RESPONSE_DELAY_SECONDS = parse_int(os.getenv("RESPONSE_DELAY_SECONDS"), 3)
LONG_POLL_TIMEOUT = parse_int(os.getenv("LONG_POLL_TIMEOUT"), 50)
HTTP_PORT = parse_int(os.getenv("PORT"), 8080)
MANAGER_USER_IDS = parse_int_set(os.getenv("MANAGER_USER_IDS"))
MANAGER_USERNAMES = parse_str_set(os.getenv("MANAGER_USERNAMES"))

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is required")
if OWNER_CHAT_ID == 0:
    raise RuntimeError("OWNER_CHAT_ID is required")

API_BASE = f"https://api.telegram.org/bot{TOKEN}"

TASK_TRIGGERS = [
    "нужно", "надо", "что делаем", "делаем", "сделать", "надо сделать", "нужно сделать",
    "берем в работу", "берём в работу", "взять в работу", "в работу", "подхватить",
    "дедлайн", "сегодня до", "завтра до", "до ", "сегодня", "завтра", "срочно", "asap",
    "заходим сюда", "зайдите", "зайти", "заполняем", "заполнить", "проверить", "проверьте",
    "подготовить", "создать", "собрать", "почистить", "обновить", "внести", "отправить",
    "отписываемся", "отпишитесь", "по готовности", "реестр", "операционк", "таблица",
    "табличка", "список", "документ", "файл", "ссылка", "отвал", "q1", "q2", "план",
    "оверплан", "контроль", "закрыть", "завести", "сверить", "пройтись", "разобрать",
]

LINK_TRIGGERS = [
    "http://", "https://", "docs.google.com", "spreadsheets", "drive.google.com", "notion.so",
]

MANAGER_REPLY_VARIANTS = [
    "Принял. Беру в работу.",
    "Принял, подхватил. Вернусь со статусом.",
    "Взял в работу. По готовности отпишусь.",
    "Принял задачу. Двигаюсь по ней.",
    "Понял, забрал. Отпишусь по результату.",
    "Ок, увидел. Беру на себя и вернусь с апдейтом.",
    "Задачу увидел, принял. Дальше уже в работе.",
    "Принял. Пройдусь по задаче и дам статус.",
    "В работе. Как будет промежуточный результат — отпишусь.",
    "Подхватил. Проверю, соберу и вернусь с обратной связью.",
    "Принял, забираю в работу. По готовности отмечусь.",
    "Ок, взял. Сначала разберу вводные, потом дам статус.",
    "Понял задачу. Уже беру в обработку.",
    "Принял. Если по пути понадобится уточнение — быстро вернусь.",
    "Вижу задачу. Беру в работу и держу в фокусе.",
    "Забрал. Дам апдейт, как только дойду до результата.",
    "Ок, понял. Подключаюсь и отпишусь по готовности.",
    "Принял вводные. Начинаю работу.",
    "Взял на контроль. По статусу вернусь отдельно.",
    "Ок, задача у меня. Отпишусь, как продвинусь.",
]

MANAGER_REPLY_DEADLINE_VARIANTS = [
    "Принял. Беру в приоритет, чтобы уложиться в дедлайн.",
    "Ок, вижу дедлайн. Подхватил и иду в работу.",
    "Принял. Держу срок в фокусе, по готовности отпишусь.",
    "Взял. Иду с приоритетом, чтобы закрыть в обозначенное время.",
    "Понял, срок вижу. Беру задачу и вернусь со статусом.",
]

MANAGER_REPLY_SHEET_VARIANTS = [
    "Принял. Зайду в таблицу, заполню нужное и отпишусь.",
    "Ок, увидел ссылку. Подхватываю таблицу и вернусь со статусом.",
    "Взял в работу. Пройдусь по таблице и отмечусь по готовности.",
    "Принял. Документ подхватил, дальше уже заполняю и сверяю.",
    "Понял. Зайду в файл, отработаю и дам апдейт.",
]

JOKES = [
    "— Почему аналитик не спорит с таблицей?\n— Потому что у таблицы всегда есть аргументы по столбцам.",
    "— Почему менеджер любит дедлайны?\n— Они, как выходные: постоянно куда-то исчезают.",
    "— Как называется человек, который всё записывает в Excel?\n— Хранитель священных ячеек.",
    "— Почему бот не опаздывает на встречу?\n— Потому что у него нет варианта “ещё пять минут”.",
    "— Что говорит отчёт утром?\n— Открой меня аккуратно, внутри сюрпризы.",
    "— Почему таблица была спокойна?\n— Она знала, что всё по полочкам и по строкам.",
    "— Как понять, что задача серьёзная?\n— Про неё сказали “маленькая просьба”.",
    "— Почему у менеджера всегда открыт календарь?\n— Потому что спонтанность уже занята с 14:00 до 15:00.",
    "— Чем отличается хороший созвон от магии?\n— После хорошего созвона хотя бы понятно, кто что делает.",
    "— Почему бот всем нравится?\n— Он не перебивает и не просит “созвонимся на минутку” каждые десять минут.",
    "— Какой любимый спорт у коллег?\n— Перекладывание задач… в статус “в работе”.",
    "— Почему документ не хотел закрываться?\n— Он чувствовал, что его ещё попросят “быстро чуть-чуть поправить”.",
    "— Что самое стабильное в проекте?\n— Фраза “там небольшая доработка”.",
    "— Почему KPI любит тишину?\n— В тишине лучше слышно, как к нему приближается отчётный период.",
    "— Как бот поднимает лояльность в команде?\n— Закидывает анекдот и не назначает встречу по итогам.",
    "— Почему коллеги любят краткие сообщения?\n— Потому что длинные обычно заканчиваются словом “срочно”.",
    "— Что общего у кофе и дедлайна?\n— Оба сначала бодрят, потом заканчиваются внезапно.",
    "— Почему задача не боялась проверки?\n— Она уже прошла через три чата и двух руководителей.",
    "— Зачем нужен порядок в файлах?\n— Чтобы в момент паники хотя бы паниковать структурированно.",
    "— Почему у бота хорошая память?\n— Он не хранит обиды, только message_id.",
]


@dataclass
class PendingReply:
    due_at: float
    chat_id: int
    thread_id: int | None
    message_id: int
    response_text: str
    notify_text: str


@dataclass
class RuntimeState:
    update_offset: int = 0
    auto_reply_enabled: bool = AUTO_REPLY_ENABLED
    pending: dict[tuple[int, int], PendingReply] = field(default_factory=dict)
    answered_message_ids: set[tuple[int, int]] = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)


state = RuntimeState()


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize(text: str) -> str:
    text = text.lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean(text: str) -> str:
    text = normalize(text)
    text = re.sub(r"[^a-zа-я0-9_:/?.#\- ]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def save_state() -> None:
    payload = {
        "update_offset": state.update_offset,
        "auto_reply_enabled": state.auto_reply_enabled,
        "saved_at": utc_now_str(),
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_state() -> None:
    if not STATE_PATH.exists():
        return
    raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    state.update_offset = int(raw.get("update_offset", 0))
    state.auto_reply_enabled = bool(raw.get("auto_reply_enabled", AUTO_REPLY_ENABLED))


def extract_text(message: dict[str, Any]) -> str:
    return str(message.get("text") or message.get("caption") or "").strip()


def extract_links(text: str) -> list[str]:
    return re.findall(r"https?://\S+", text)


def choose(items: list[str]) -> str:
    return random.choice(items)


def allowed_chat(chat_id: int) -> bool:
    return not ALLOWED_CHAT_IDS or chat_id in ALLOWED_CHAT_IDS


def format_sender_name(user: dict[str, Any] | None) -> str:
    if not user:
        return "Неизвестный"
    first = str(user.get("first_name") or "").strip()
    last = str(user.get("last_name") or "").strip()
    full = " ".join(x for x in [first, last] if x).strip()
    if full:
        return full
    username = str(user.get("username") or "").strip()
    if username:
        return f"@{username}"
    return f"user_{user.get('id', 'unknown')}"


def is_manager_message(user: dict[str, Any]) -> bool:
    user_id = int(user.get("id", 0))
    username = str(user.get("username") or "").strip().lstrip("@").lower()
    if MANAGER_USER_IDS and user_id in MANAGER_USER_IDS:
        return True
    if MANAGER_USERNAMES and username and username in MANAGER_USERNAMES:
        return True
    return False


def has_attachment(message: dict[str, Any]) -> bool:
    attachment_keys = [
        "document", "photo", "video", "audio", "voice", "animation", "sticker",
        "poll", "contact", "location",
    ]
    return any(key in message for key in attachment_keys)


def looks_like_task_from_manager(message: dict[str, Any]) -> bool:
    text = extract_text(message)
    cleaned = clean(text)
    if has_attachment(message):
        return True
    if any(token in cleaned for token in TASK_TRIGGERS):
        return True
    if any(token in cleaned for token in LINK_TRIGGERS):
        return True
    if text and len(text) >= 140:
        return True
    if message.get("reply_to_message") and text and len(text) >= 25:
        return True
    return False


def choose_manager_reply(message: dict[str, Any]) -> str:
    text = clean(extract_text(message))
    if any(token in text for token in ["дедлайн", "сегодня до", "завтра до", "срочно", "до "]):
        return choose(MANAGER_REPLY_DEADLINE_VARIANTS)
    if has_attachment(message) or any(token in text for token in ["таблица", "табличка", "реестр", "docs.google.com", "spreadsheets", "файл", "документ"]):
        return choose(MANAGER_REPLY_SHEET_VARIANTS)
    return choose(MANAGER_REPLY_VARIANTS)


def manager_notify_text(message: dict[str, Any], response_text: str) -> str:
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    text = extract_text(message)
    links = extract_links(text)
    scope = f"Тема {message.get('message_thread_id')}" if message.get("message_thread_id") else "Без темы"
    attachment = "да" if has_attachment(message) else "нет"
    parts = [
        "Николай: ответил на сообщение руководителя.",
        f"Чат: {chat.get('title') or chat.get('id')}",
        f"Контекст: {scope}",
        f"От: {format_sender_name(sender)}",
        f"Вложение: {attachment}",
    ]
    if links:
        parts.append(f"Ссылка: {links[0]}")
    if text:
        parts.append(f"Сообщение: {text[:1200]}")
    parts.append(f"Ответ: {response_text}")
    return "\n".join(parts)


def queue_reply(reply: PendingReply) -> None:
    key = (reply.chat_id, reply.message_id)
    with state.lock:
        state.pending[key] = reply


def process_owner_command(message: dict[str, Any]) -> None:
    global OWNER_CHAT_ID

    user = message.get("from") or {}
    user_id = int(user.get("id", 0))
    chat_id = int((message.get("chat") or {}).get("id", 0))
    text = extract_text(message)

    if OWNER_USER_ID and user_id != OWNER_USER_ID:
        send_message(chat_id, "У тебя нет прав на управление этим ботом.")
        return

    if text.startswith("/start"):
        OWNER_CHAT_ID = chat_id
        send_message(chat_id, "Николай на связи. Команды: /status, /pause, /resume, /reload, /test. Шутки в группе: /anekdot, /анекдот, /joke")
        return

    if text.startswith("/status"):
        with state.lock:
            pending_count = len(state.pending)
            auto_mode = state.auto_reply_enabled
        send_message(
            chat_id,
            (
                f"Статус: {'включен' if auto_mode else 'пауза'}\n"
                f"В очереди: {pending_count}\n"
                f"Чаты: {', '.join(str(x) for x in sorted(ALLOWED_CHAT_IDS)) if ALLOWED_CHAT_IDS else 'все'}\n"
                f"Руководитель: ids={sorted(MANAGER_USER_IDS)} usernames={sorted(MANAGER_USERNAMES)}\n"
                f"Анекдоты: {len(JOKES)}"
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
        load_state()
        send_message(chat_id, "Конфиг перечитан. Текущий код работает.")
        return

    if text.startswith("/test"):
        send_message(chat_id, "Тестовое уведомление: я работаю.")
        return

    if text.startswith("/help"):
        send_message(chat_id, "Команды: /status, /pause, /resume, /reload, /test. В группе: /anekdot, /анекдот, /joke")
        return


def process_group_fun_commands(message: dict[str, Any]) -> bool:
    text = extract_text(message)
    if not text:
        return False
    normalized = text.strip().lower()
    if normalized.startswith("/anekdot") or normalized.startswith("/анекдот") or normalized.startswith("/joke"):
        send_message(
            int((message.get("chat") or {}).get("id", 0)),
            choose(JOKES),
            thread_id=message.get("message_thread_id"),
            reply_to_message_id=message.get("message_id"),
        )
        return True
    return False


def maybe_queue_manager_reply(message: dict[str, Any]) -> None:
    user = message.get("from") or {}
    if not is_manager_message(user):
        return
    if not looks_like_task_from_manager(message):
        return

    chat_id = int((message.get("chat") or {}).get("id", 0))
    message_id = int(message.get("message_id", 0))
    key = (chat_id, message_id)
    with state.lock:
        if key in state.answered_message_ids or key in state.pending:
            return

    response_text = choose_manager_reply(message)
    notify_text = manager_notify_text(message, response_text)
    reply = PendingReply(
        due_at=time.time() + max(RESPONSE_DELAY_SECONDS, 0),
        chat_id=chat_id,
        thread_id=message.get("message_thread_id"),
        message_id=message_id,
        response_text=response_text,
        notify_text=notify_text,
    )
    queue_reply(reply)
    logger.info("Queued manager reply for message %s in chat %s", message_id, chat_id)


def handle_message(message: dict[str, Any]) -> None:
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    if not chat or not user:
        return

    chat_id = int(chat.get("id", 0))
    chat_type = str(chat.get("type", ""))
    is_bot = bool(user.get("is_bot", False))
    if is_bot:
        return

    text_preview = extract_text(message)[:200]
    logger.info("Incoming | chat=%s | type=%s | user=%s | text=%s", chat_id, chat_type, user.get("id"), text_preview)

    if chat_type == "private":
        process_owner_command(message)
        return

    if not allowed_chat(chat_id):
        return

    if process_group_fun_commands(message):
        return

    with state.lock:
        auto_mode = state.auto_reply_enabled
    if not auto_mode:
        return

    maybe_queue_manager_reply(message)


def flush_pending() -> None:
    ready: list[PendingReply] = []
    with state.lock:
        for key, reply in list(state.pending.items()):
            if reply.due_at <= time.time():
                ready.append(reply)
                del state.pending[key]

    for reply in ready:
        key = (reply.chat_id, reply.message_id)
        try:
            send_message(
                reply.chat_id,
                reply.response_text,
                thread_id=reply.thread_id,
                reply_to_message_id=reply.message_id,
            )
            send_message(OWNER_CHAT_ID, reply.notify_text)
            with state.lock:
                state.answered_message_ids.add(key)
        except Exception as exc:
            logger.exception("Failed to send reply: %s", exc)
            try:
                send_message(OWNER_CHAT_ID, f"Николай: не смог ответить на сообщение {reply.message_id} в чате {reply.chat_id}. Ошибка: {exc}")
            except Exception:
                logger.exception("Failed to notify owner about send failure")


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
            "pending": len(state.pending),
            "allowed_chat_ids": sorted(ALLOWED_CHAT_IDS),
            "manager_ids": sorted(MANAGER_USER_IDS),
            "manager_usernames": sorted(MANAGER_USERNAMES),
            "anecdotes": len(JOKES),
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
    threading.Thread(target=poller_loop, daemon=True).start()
    serve_http()


if __name__ == "__main__":
    main()
