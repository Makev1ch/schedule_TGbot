#!/usr/bin/env python3
import asyncio
import copy
import csv
import io
import logging
import json
import html
import os
import re
import random
import secrets
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, List, Tuple, Dict
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile, BotCommand, BotCommandScopeChat, KeyboardButton, Message, ReplyKeyboardMarkup
from bs4 import BeautifulSoup

from database_stable import Database, UserSettingsStore, MySQLStorage

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# ==================== CONFIG ====================
try:
    IRKUTSK_TZ = ZoneInfo("Asia/Irkutsk")
except ZoneInfoNotFoundError:
    IRKUTSK_TZ = timezone(timedelta(hours=8))

BASE_SCHEDULE_URL = "https://www.istu.edu/raspisanie/"
SEARCH_URL = "https://www.istu.edu/raspisanie/poisk"
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID") or "1307617601")
USER_IDS_FILE = Path(__file__).with_name("telegram_ids.txt")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
REQUEST_TIMEOUT = 15.0
FSM_CLEANUP_INTERVAL = 3 * 60 * 60  # 3 часа

# Подтверждение рассылки действительно 10 минут.
BROADCAST_CONFIRM_TTL_SECONDS = 10 * 60

# Меняется при каждом запуске процесса: незавершённая рассылка
# из старого процесса не сможет быть подтверждена после перезапуска.
BROADCAST_SESSION_TOKEN = secrets.token_hex(16)

# Защита от параллельного запуска рассылок.
BROADCAST_LOCK = asyncio.Lock()

# Защита от повторной обработки одного альбома
_REJECTED_MEDIA_GROUPS: set[str] = set()

# ==================== REGEX ====================
RE_TIME = re.compile(r"^(\d{1,2}):(\d{2})$")
RE_SUBGROUP = re.compile(r"подгруппа\s*(\d+)", re.IGNORECASE)
RE_KIND_LECTURE = re.compile(r"лекц", re.IGNORECASE)
RE_KIND_PRACTICE = re.compile(r"практ", re.IGNORECASE)
RE_KIND_LAB = re.compile(r"лаб(?:оратор)?", re.IGNORECASE)
RE_DAY_MONTH = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
RE_COURSE_ID = re.compile(r"^Курс\s*(\d+)\b", re.IGNORECASE)
RE_SUBDIV_PATH = re.compile(r"/raspisanie/podrazdelenie/(\d+)")
RE_GROUP_PATH = re.compile(r"/raspisanie/grup/(\d+)")
RE_PREP_PATH = re.compile(r"/raspisanie/prepodavatel/(\d+)")

RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

# ==================== BUTTONS ====================
BTN_TODAY = "📆 На сегодня"
BTN_TOMORROW = "⏭️ На завтра"
BTN_THIS_WEEK = "📆 На текущую неделю"
BTN_NEXT_WEEK = "⏭️ На следующую неделю"
BTN_CHANGE_GROUP = "🔁 Изменить группу"
BTN_REPORT = "🐞 Сообщить о проблеме"
BTN_BACK = "⬅️ Назад"
BTN_PAGE_PREV = "⬅️ Пред. страница"
BTN_PAGE_NEXT = "➡️ След. страница"
BTN_CANCEL = "❌ Отмена"
BTN_TEACHER_SCHEDULE = "👨‍🏫 Расписание преподавателей"
BTN_GROUP_SCHEDULE = "👥 Расписание группы"
BTN_CHANGE_TEACHER = "🔁 Сменить преподавателя"

ALL_BTNS = {BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK,
            BTN_CHANGE_GROUP, BTN_REPORT, BTN_BACK, BTN_PAGE_PREV, BTN_PAGE_NEXT, BTN_CANCEL,
            BTN_TEACHER_SCHEDULE, BTN_GROUP_SCHEDULE, BTN_CHANGE_TEACHER}

MENU_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_GROUP), KeyboardButton(text=BTN_REPORT)],
    ],
    resize_keyboard=True,
)

MENU_KB_GROUP = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_GROUP), KeyboardButton(text=BTN_REPORT)],
        [KeyboardButton(text=BTN_TEACHER_SCHEDULE)],
    ],
    resize_keyboard=True,
)

MENU_KB_TEACHER = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_TEACHER), KeyboardButton(text=BTN_REPORT)],
        [KeyboardButton(text=BTN_GROUP_SCHEDULE)],
    ],
    resize_keyboard=True,
)

# ==================== ADMIN PANEL BUTTONS ====================
BTN_ADM_BROADCAST = "📢 Рассылка всем"
BTN_ADM_TOGGLE = "🧪 Тестовый режим"
BTN_ADM_TEST_LIST = "👥 Тестовый список"
BTN_ADM_ADD_ID = "➕ Добавить ID"
BTN_ADM_CLOSE = "🚪 Выйти из панели"

BTN_BROADCAST_CONFIRM = "✅ Да, отправить"
BTN_BROADCAST_CANCEL = "❌ Нет, отменить"

# ==================== FSM ====================
class SetupFlow(StatesGroup):
    institute = State()
    course = State()
    group = State()

class ReportFlow(StatesGroup):
    report = State()

class TeacherFlow(StatesGroup):
    search = State()
    select = State()

class BroadcastFlow(StatesGroup):
    waiting_text = State()
    confirm = State()

class AdminPanel(StatesGroup):
    menu = State()
    test_list = State()
    waiting_new_test_id = State()

# ==================== MODELS ====================
class Institute:
    __slots__ = ('subdiv_id', 'title')
    def __init__(self, subdiv_id: int, title: str):
        self.subdiv_id = subdiv_id
        self.title = title

class Group:
    __slots__ = ('group_id', 'title')
    def __init__(self, group_id: int, title: str):
        self.group_id = group_id
        self.title = title

class Lesson:
    __slots__ = ('start', 'subject', 'kind', 'subgroup', 'room', 'teacher', 'group_name')
    def __init__(self, start: time, subject: str, kind: str, subgroup: str, room: str, teacher: str, group_name: str = ""):
        self.start = start
        self.subject = subject
        self.kind = kind
        self.subgroup = subgroup
        self.room = room
        self.teacher = teacher
        self.group_name = group_name

class DaySchedule:
    __slots__ = ('heading', 'lessons', 'date_str')
    def __init__(self, heading: str, lessons: List[Lesson], date_str: str = ""):
        self.heading = heading
        self.lessons = lessons
        self.date_str = date_str

class Teacher:
    __slots__ = ('prep_id', 'name')
    def __init__(self, prep_id: int, name: str):
        self.prep_id = prep_id
        self.name = name

# ==================== HELPERS ====================
def iso_week_key(d: date) -> Tuple[int, int]:
    return d.isocalendar().year, d.isocalendar().week

def is_odd_week(d: date) -> bool:
    return d.isocalendar().week % 2 == 1

def _parse_time(value: str) -> Optional[time]:
    m = RE_TIME.match(value.strip())
    if not m:
        return None
    h, mm = int(m.group(1)), int(m.group(2))
    return time(hour=h, minute=mm) if 0 <= h <= 23 and 0 <= mm <= 59 else None

def _extract_subgroup(text: str) -> str:
    m = RE_SUBGROUP.search(text)
    return f"подгруппа {m.group(1)}" if m else ""

def _subgroup_sort_key(subgroup: str) -> tuple[int, int]:
    if not subgroup:
        return (1, 0)
    m = RE_SUBGROUP.search(subgroup) or re.search(r"(\d+)", subgroup)
    if not m:
        return (1, 0)
    return (0, int(m.group(1)))

def _extract_lesson_kind(text: str) -> str:
    if RE_KIND_LECTURE.search(text):
        return "лекция"
    if RE_KIND_PRACTICE.search(text):
        return "практика"
    if RE_KIND_LAB.search(text):
        return "лаба"
    return ""

def _parse_date_from_string(date_str: str) -> Optional[date]:
    m = RE_DAY_MONTH.match(date_str.strip())
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None

def _format_day_message(heading: str, lessons: List[Lesson]) -> str:
    sep = "------------------------------"
    out: List[str] = [f"🍌{html.escape(heading)}", sep]
    if not lessons:
        out.append("нет занятий")
        return "\n".join(out)

    lessons.sort(key=lambda l: (l.start, _subgroup_sort_key(l.subgroup), l.subject.lower(), l.kind, l.room, l.teacher))
    blocks: dict[time, List[Lesson]] = {}
    order: List[time] = []
    for lesson in lessons:
        if lesson.start not in blocks:
            blocks[lesson.start] = []
            order.append(lesson.start)
        blocks[lesson.start].append(lesson)

    for i, start_t in enumerate(order):
        start_dt = datetime.combine(date(2000, 1, 1), start_t)
        end_dt = start_dt + timedelta(minutes=90)
        for j, lesson in enumerate(blocks[start_t]):
            if j > 0:
                out.append("===== ")
            safe_subject = html.escape(lesson.subject or "—")
            safe_kind = html.escape(lesson.kind or "")
            kind_part = f" ({safe_kind})" if safe_kind else ""
            out.append(f"{start_t.strftime('%H:%M')} — {end_dt.time().strftime('%H:%M')} {safe_subject}{kind_part}")
            details = [d for d in [
                html.escape(lesson.subgroup) if lesson.subgroup else None,
                html.escape(lesson.room) if lesson.room != "—" else None,
                html.escape(lesson.teacher) if lesson.teacher != "—" else None
            ] if d]
            if details:
                out.append(" • " + " | ".join(details))
        if i < len(order) - 1:
            out.append(sep)
    return "\n".join(out)

def _format_day_message_teacher(heading: str, lessons: List[Lesson]) -> str:
    sep = "---------------------"
    out: List[str] = [f"🍌{html.escape(heading)}", sep]
    if not lessons:
        out.append("нет занятий")
        return "\n".join(out)

    lessons.sort(key=lambda l: (l.start, _subgroup_sort_key(l.subgroup), l.subject.lower(), l.kind, l.room))
    blocks: dict[time, List[Lesson]] = {}
    order: List[time] = []
    for lesson in lessons:
        if lesson.start not in blocks:
            blocks[lesson.start] = []
            order.append(lesson.start)
        blocks[lesson.start].append(lesson)

    for i, start_t in enumerate(order):
        start_dt = datetime.combine(date(2000, 1, 1), start_t)
        end_dt = start_dt + timedelta(minutes=90)
        for j, lesson in enumerate(blocks[start_t]):
            if j > 0:
                out.append("===== ")
            safe_subject = html.escape(lesson.subject or "—")
            safe_kind = html.escape(lesson.kind or "")
            kind_part = f" ({safe_kind})" if safe_kind else ""
            out.append(f"{start_t.strftime('%H:%M')} — {end_dt.time().strftime('%H:%M')} {safe_subject}{kind_part}")
            details = [d for d in [
                html.escape(lesson.group_name) if lesson.group_name else None,
                html.escape(lesson.subgroup) if lesson.subgroup else None,
                html.escape(lesson.room) if lesson.room != "—" else None,
            ] if d]
            if details:
                out.append(" • " + " | ".join(details))
        if i < len(order) - 1:
            out.append(sep)
    return "\n".join(out)

def _chunk(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]

def _is_sshg(title: str) -> bool:
    t = title.lower()
    return "сибирская школа геонаук" in t or "siberian school of geosciences" in t

# ==================== KEYBOARD BUILDERS ====================
def build_paged_kb(options: List[str], page: int, page_size: int, row_size: int, show_back: bool) -> ReplyKeyboardMarkup:
    start = page * page_size
    slice_opts = options[start:start + page_size]
    keyboard: List[List[KeyboardButton]] = []
    for row in _chunk(slice_opts, row_size):
        keyboard.append([KeyboardButton(text=t) for t in row])
    controls = []
    if page > 0:
        controls.append(KeyboardButton(text=BTN_PAGE_PREV))
    if start + page_size < len(options):
        controls.append(KeyboardButton(text=BTN_PAGE_NEXT))
    if controls:
        keyboard.append(controls)
    keyboard.append([KeyboardButton(text=BTN_REPORT)])
    if show_back:
        keyboard.append([KeyboardButton(text=BTN_BACK)])
    keyboard.append([KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def build_teacher_select_kb(teacher_names: List[str]) -> ReplyKeyboardMarkup:
    keyboard: List[List[KeyboardButton]] = []
    for name in teacher_names:
        keyboard.append([KeyboardButton(text=name)])
    keyboard.append([KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def build_admin_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text=BTN_ADM_BROADCAST)],
        [KeyboardButton(text=BTN_ADM_TOGGLE)],
        [KeyboardButton(text=BTN_ADM_TEST_LIST)],
        [KeyboardButton(text=BTN_ADM_CLOSE)],
    ], resize_keyboard=True)


def build_test_list_kb(users: list) -> ReplyKeyboardMarkup:
    keyboard = [[KeyboardButton(text=f"❌ {label or uid}")] for uid, label in users]
    keyboard.append([KeyboardButton(text=BTN_ADM_ADD_ID)])
    keyboard.append([KeyboardButton(text=BTN_BACK)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# ==================== REFERENCE DATA CACHE ====================
class ReferenceDataCache:
    def __init__(self, schedule_client: "ScheduleClient"):
        self._client = schedule_client
        self._institutes: Optional[List[Institute]] = None
        self._institutes_loaded_at: Optional[float] = None
        self._institutes_ttl = 1800.0
        self._groups_cache: Dict[int, Dict[int, List[Group]]] = {}
        self._groups_loaded_at: Dict[int, float] = {}
        self._groups_ttl = 600.0
        self._inst_by_label: Dict[str, Institute] = {}

    def _current_time(self) -> float:
        return asyncio.get_running_loop().time()

    async def get_institutes(self) -> List[Institute]:
        now = self._current_time()
        if self._institutes is not None and self._institutes_loaded_at:
            if now - self._institutes_loaded_at < self._institutes_ttl:
                return self._institutes
        self._institutes = await self._client.list_institutes()
        self._institutes_loaded_at = now
        self._inst_by_label = {}
        for inst in self._institutes:
            label = "СШГ" if _is_sshg(inst.title) else inst.title
            self._inst_by_label[label] = inst
        return self._institutes

    def get_institute_labels(self) -> List[str]:
        return list(self._inst_by_label.keys())

    def find_institute_by_label(self, label: str) -> Optional[Institute]:
        return self._inst_by_label.get(label)

    async def get_groups_by_course(self, subdiv_id: int) -> Dict[int, List[Group]]:
        now = self._current_time()
        if subdiv_id in self._groups_cache:
            if now - self._groups_loaded_at.get(subdiv_id, 0) < self._groups_ttl:
                return self._groups_cache[subdiv_id]
        by_course = await self._client.list_groups_by_course(subdiv_id)
        self._groups_cache[subdiv_id] = by_course
        self._groups_loaded_at[subdiv_id] = now
        return by_course

    def get_cached_groups(self, subdiv_id: int) -> Optional[Dict[int, List[Group]]]:
        return self._groups_cache.get(subdiv_id)

# ==================== NAVIGATION LOGIC ====================
async def handle_navigation(
    message: Message, state: FSMContext, options: List[str], page_key: str,
    page_size: int, row_size: int, back_state: Optional[State] = None,
    back_options: Optional[List[str]] = None
) -> bool:
    text = message.text
    if text == BTN_CANCEL:
        fsm_data = await state.get_data()
        await state.clear()
        kb = MENU_KB_TEACHER if fsm_data.get("mode") == "teacher" else MENU_KB_GROUP
        await message.answer("Ок", reply_markup=kb)
        return True
    if text == BTN_BACK and back_state:
        await state.set_state(back_state)
        if back_options:
            await message.answer("Выбери:", reply_markup=build_paged_kb(back_options, 0, page_size, 1, False))
        return True
    if text in (BTN_PAGE_PREV, BTN_PAGE_NEXT):
        data = await state.get_data()
        current_page = data.get(page_key, 0)
        if text == BTN_PAGE_PREV:
            new_page = max(0, current_page - 1)
        else:
            max_page = (len(options) - 1) // page_size
            new_page = min(max_page, current_page + 1)
        await state.update_data({page_key: new_page})
        await message.answer("Выбери:", reply_markup=build_paged_kb(options, new_page, page_size, row_size, bool(back_state)))
        return True
    return False

async def safe_send(message: Message, text: str, limit: int = 3500):
    parts = []
    while len(text) > limit:
        split_pos = text.rfind("\n", 0, limit)
        if split_pos == -1:
            split_pos = limit
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip()
    if text:
        parts.append(text)
    for part in parts:
        await message.answer(part)

class NoScheduleFound(Exception):
    pass

# ==================== SCHEDULE CLIENT ====================
class ScheduleClient:
    def __init__(self, session: aiohttp.ClientSession):
        self._session = session
        self._cache: dict[str, tuple[float, tuple[bool, List[DaySchedule]]]] = {}
        self._cache_ttl = 120.0

    def _prune(self):
        now = asyncio.get_running_loop().time()
        for k, (ts, _) in list(self._cache.items()):
            if now - ts > self._cache_ttl:
                self._cache.pop(k, None)

    async def _fetch(self, url: str, params: dict = None, data: dict = None) -> str:
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                headers = {"User-Agent": "ISTU-Bot/2.3", "Connection": "close"}
                if data:
                    async with self._session.post(url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                        if resp.status >= 500:
                            raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status, message="Server Error")
                        resp.raise_for_status()
                        return await resp.text()
                else:
                    async with self._session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                        if resp.status >= 500:
                            raise aiohttp.ClientResponseError(resp.request_info, resp.history, status=resp.status, message="Server Error")
                        resp.raise_for_status()
                        return await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                delay = min(RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1), 10.0)
                logging.warning(f"Retry {attempt+1}/{MAX_RETRIES} in {delay:.1f}s: {e}")
                await asyncio.sleep(delay)
        raise last_exc or RuntimeError("Network error")

    async def list_institutes(self) -> List[Institute]:
        html_content = await self._fetch(BASE_SCHEDULE_URL, {})
        soup = BeautifulSoup(html_content, "html.parser")
        inst: List[Institute] = []
        seen_ids: set = set()
        for a in soup.select('a[href*="/raspisanie/podrazdelenie/"]'):
            href = a.get("href") or ""
            m = RE_SUBDIV_PATH.search(href)
            if not m: continue
            sid = int(m.group(1))
            if sid in seen_ids: continue
            seen_ids.add(sid)
            title = a.get_text(" ", strip=True)
            if title:
                inst.append(Institute(sid, title))
        inst.sort(key=lambda x: x.title.lower())
        return inst

    async def list_groups_by_course(self, subdiv_id: int) -> dict[int, List[Group]]:
        url = f"{BASE_SCHEDULE_URL}podrazdelenie/{subdiv_id}"
        html_content = await self._fetch(url, {})
        soup = BeautifulSoup(html_content, "html.parser")
        by_course: dict[int, List[Group]] = {}
        for kurs_block in soup.select("div.schd-kurs-block"):
            kurs_num_el = kurs_block.select_one("div.schd-kurs-nuber")
            if not kurs_num_el: continue
            kurs_text = kurs_num_el.get_text(strip=True)
            m = re.search(r"(\d+)", kurs_text)
            if not m: continue
            course = int(m.group(1))
            groups: List[Group] = []
            groups_div = kurs_block.select_one("div.schd-kurs-groups")
            if not groups_div: continue
            for grp_item in groups_div.select("div.schd-grp-item a"):
                href = grp_item.get("href") or ""
                gm = RE_GROUP_PATH.search(href)
                if not gm: continue
                gid = int(gm.group(1))
                title = grp_item.get_text(strip=True)
                if title:
                    groups.append(Group(gid, title))
            if groups:
                groups.sort(key=lambda g: g.title.lower())
                by_course[course] = groups
        return by_course

    async def get_week_schedule(self, group_id: int, target_date: date) -> tuple[bool, List[DaySchedule]]:
        self._prune()
        key = f"{group_id}:{target_date.isoformat()}"
        if cached := self._cache.get(key):
            # Глубокое копирование для предотвращения мутации кэша
            return copy.deepcopy(cached[1])
        url = f"{BASE_SCHEDULE_URL}grup/{group_id}/{target_date.strftime('%d.%m.%Y')}/"
        html_content = await self._fetch(url)
        result = self._parse_schedule_html(html_content, target_date)
        self._cache[key] = (asyncio.get_running_loop().time(), result)
        return copy.deepcopy(result)

    async def get_teacher_week_schedule(self, prep_id: int, target_date: date) -> tuple[bool, List[DaySchedule]]:
        self._prune()
        key = f"prep:{prep_id}:{target_date.isoformat()}"
        if cached := self._cache.get(key):
            return copy.deepcopy(cached[1])
        url = f"{BASE_SCHEDULE_URL}prepodavatel/{prep_id}/{target_date.strftime('%d.%m.%Y')}/"
        html_content = await self._fetch(url)
        result = self._parse_schedule_html(html_content, target_date)
        self._cache[key] = (asyncio.get_running_loop().time(), result)
        return copy.deepcopy(result)

    def _parse_schedule_html(self, html_content: str, target_date: date) -> tuple[bool, List[DaySchedule]]:
        soup = BeautifulSoup(html_content, "html.parser")
        days: List[DaySchedule] = []
        target_is_odd = is_odd_week(target_date)
        for day_div in soup.select("div.sch-list-day"):
            params_str = day_div.get("data-params", "{}")
            try:
                params = json.loads(params_str.replace("'", '"'))
                date_str = params.get("date", "")
            except Exception:
                date_str = ""
            heading_el = day_div.select_one("h2")
            heading = heading_el.get_text(" ", strip=True) if heading_el else "Неизвестный день"
            lessons: List[Lesson] = []
            for item_div in day_div.select("div.sch-list-item"):
                item_params_str = item_div.get("data-params", "{}")
                try:
                    item_params = json.loads(item_params_str.replace("'", '"'))
                    time_str = item_params.get("time", "")
                except Exception:
                    time_str = ""
                start = _parse_time(time_str) if time_str else None
                if not start:
                    continue
                week_blocks = []
                for wb in item_div.select("div.sch-list-item-week"):
                    classes = wb.get("class", [])
                    if "week-all" in classes:
                        week_blocks.append(wb)
                        continue
                    if target_is_odd and "week-odd" in classes:
                        week_blocks.append(wb)
                        continue
                    if not target_is_odd and "week-even" in classes:
                        week_blocks.append(wb)
                        continue
                for week_block in week_blocks:
                    for schcls in week_block.select("div.schcls-item"):
                        if "schcls-empty" in schcls.get("class", []):
                            continue
                        subject_el = schcls.select_one("div.schcls-item-name")
                        subject = subject_el.get_text(" ", strip=True) if subject_el else ""
                        if not subject:
                            continue
                        distype_el = schcls.select_one("div.schcls-item-distype")
                        kind_raw = distype_el.get_text(" ", strip=True).lower() if distype_el else ""
                        kind = _extract_lesson_kind(kind_raw)
                        teacher = "—"
                        prepod_el = schcls.select_one("div.schcls-item-prepod")
                        if prepod_el:
                            prepod_link = prepod_el.select_one("a")
                            if prepod_link:
                                teacher = prepod_link.get_text(" ", strip=True)
                        if not teacher:
                            teacher = "—"
                        group_name = ""
                        subgroup = ""
                        group_el = schcls.select_one("div.schcls-item-group")
                        if group_el:
                            group_links = group_el.select("a")
                            groups = [a.get_text(" ", strip=True) for a in group_links if a.get_text(strip=True)]
                            group_name = ", ".join(groups)
                            full_group_text = group_el.get_text(" ", strip=True)
                            subgroup_match = re.search(r"подгруппа\s*(\d+)", full_group_text, re.IGNORECASE)
                            if subgroup_match:
                                subgroup = f"подгруппа {subgroup_match.group(1)}"
                        room = "—"
                        aud_el = schcls.select_one("div.schcls-item-aud")
                        if aud_el:
                            room_text = aud_el.get_text(" ", strip=True)
                            if room_text and room_text != "-":
                                room = room_text
                        lessons.append(Lesson(start, subject, kind, subgroup, room, teacher, group_name))
            lessons.sort(key=lambda l: (l.start, l.subject.lower(), l.subgroup, l.room))
            days.append(DaySchedule(heading, lessons, date_str))
        if not days:
            logging.info("Расписание отсутствует")
            raise NoScheduleFound("No schedule found")
        return (target_is_odd, days)

    async def search_teachers(self, query: str) -> List[Teacher]:
        html_content = await self._fetch(SEARCH_URL, data={"zapros": query})
        soup = BeautifulSoup(html_content, "html.parser")
        teachers: List[Teacher] = []
        seen_ids: set = set()
        for a in soup.select('a[href*="/raspisanie/prepodavatel/"]'):
            href = a.get("href") or ""
            m = RE_PREP_PATH.search(href)
            if not m: continue
            prep_id = int(m.group(1))
            if prep_id in seen_ids: continue
            seen_ids.add(prep_id)
            name = a.get_text(" ", strip=True)
            name = re.sub(r"\s+", " ", name)
            if name:
                teachers.append(Teacher(prep_id, name))
        return teachers

# ==================== DATABASE EXTENSIONS ====================
async def init_registered_users_table(db: Database):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS registered_users (
            user_id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

async def register_user(db: Database, user_id: int):
    try:
        await db.execute("INSERT IGNORE INTO registered_users (user_id) VALUES (%s)", (user_id,))
    except Exception:
        logging.exception(f"Failed to register user {user_id}")

async def import_users_from_file(db: Database, filepath: Path):
    if not filepath.exists():
        logging.warning(f"User IDs file not found: {filepath}")
        return
    count = 0
    errors = 0
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                uid = int(line)
                await db.execute("INSERT IGNORE INTO registered_users (user_id) VALUES (%s)", (uid,))
                count += 1
            except Exception:
                errors += 1
    logging.info(f"Imported {count} user IDs from file ({errors} errors)")

async def init_broadcast_test_table(db: Database):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS broadcast_test_users (
            user_id BIGINT PRIMARY KEY,
            label VARCHAR(100) NULL,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS bot_config (
            config_key VARCHAR(50) PRIMARY KEY,
            config_value VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

async def get_test_mode(db: Database) -> bool:
    row = await db.fetchone("SELECT config_value FROM bot_config WHERE config_key = 'test_mode'")
    return bool(row and row[0] == "1")

async def set_test_mode(db: Database, enabled: bool):
    await db.execute("""
        INSERT INTO bot_config (config_key, config_value) VALUES ('test_mode', %s)
        ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)
    """, ("1" if enabled else "0",))

async def get_test_users(db: Database) -> list:
    rows = await db.fetchall("SELECT user_id, label FROM broadcast_test_users ORDER BY added_at")
    return [(r[0], r[1]) for r in (rows or [])]

async def add_test_user(db: Database, user_id: int, label: Optional[str] = None):
    await db.execute("""
        INSERT INTO broadcast_test_users (user_id, label) VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE label = VALUES(label)
    """, (user_id, label))

async def remove_test_user(db: Database, user_id: int):
    await db.execute("DELETE FROM broadcast_test_users WHERE user_id = %s", (user_id,))

# ==================== HANDLERS ====================
async def safe_request(message: Message, coro):
    try:
        return await coro
    except NoScheduleFound:
        return "NO_SCHEDULE"
    except Exception:
        logging.exception("Schedule request failed")
        await message.answer("⚠️ Не удалось связаться с сервером ИРНИТУ. Попробуй позже.")
        return None

async def cmd_start(message: Message, state: FSMContext, ref_cache: ReferenceDataCache, db: Database):
    if message.from_user:
        await register_user(db, message.from_user.id)
    await state.clear()
    await state.set_state(SetupFlow.institute)
    try:
        await ref_cache.get_institutes()
    except Exception:
        logging.exception("Failed to load institutes")
        await message.answer("⚠️ Не удалось загрузить список институтов. Попробуй позже.")
        return
    labels = ref_cache.get_institute_labels()
    await state.update_data(inst_page=0)
    await message.answer("Выбери институт:", reply_markup=build_paged_kb(labels, 0, 12, 1, False))

async def on_setup_institute(message: Message, state: FSMContext, ref_cache: ReferenceDataCache):
    labels = ref_cache.get_institute_labels()
    if await handle_navigation(message, state, labels, "inst_page", 12, 1):
        return
    selected = ref_cache.find_institute_by_label(message.text)
    if not selected:
        await message.answer("Выбери институт кнопкой.", reply_markup=build_paged_kb(labels, 0, 12, 1, False))
        return
    try:
        await ref_cache.get_groups_by_course(selected.subdiv_id)
    except Exception:
        logging.exception(f"Failed to load groups for subdiv {selected.subdiv_id}")
        await message.answer("⚠️ Не удалось загрузить список групп. Попробуй позже.")
        return
    by_course = ref_cache.get_cached_groups(selected.subdiv_id)
    courses = sorted(by_course.keys()) if by_course else []
    if not courses:
        await message.answer("Нет курсов для этого института.")
        return
    await state.set_state(SetupFlow.course)
    await state.update_data(subdiv_id=selected.subdiv_id, courses=courses, course_page=0)
    await message.answer("Выбери курс:", reply_markup=build_paged_kb([str(c) for c in courses], 0, 12, 3, True))

async def on_setup_course(message: Message, state: FSMContext, ref_cache: ReferenceDataCache):
    data = await state.get_data()
    courses = data.get("courses", [])
    subdiv_id = data.get("subdiv_id")
    course_labels = [str(c) for c in courses]
    if await handle_navigation(message, state, course_labels, "course_page", 12, 3, SetupFlow.institute, ref_cache.get_institute_labels()):
        return
    try:
        course = int(message.text)
    except ValueError:
        await message.answer("Выбери курс кнопкой.", reply_markup=build_paged_kb(course_labels, 0, 12, 3, True))
        return
    if course not in courses:
        await message.answer("Выбери курс кнопкой.", reply_markup=build_paged_kb(course_labels, 0, 12, 3, True))
        return
    by_course = ref_cache.get_cached_groups(subdiv_id)
    if not by_course:
        await message.answer("⚠️ Данные устарели. Начни заново /start")
        await state.clear()
        return
    groups = by_course.get(course, [])
    if not groups:
        await message.answer("Нет групп на этом курсе.")
        return
    await state.set_state(SetupFlow.group)
    await state.update_data(course=course, group_page=0)
    await message.answer("Выбери группу:", reply_markup=build_paged_kb([g.title for g in groups], 0, 10, 2, True))

async def on_setup_group(message: Message, state: FSMContext, ref_cache: ReferenceDataCache, store: UserSettingsStore):
    data = await state.get_data()
    subdiv_id = data.get("subdiv_id")
    course = data.get("course")
    by_course = ref_cache.get_cached_groups(subdiv_id)
    if not by_course:
        await message.answer("⚠️ Данные устарели. Начни заново /start")
        await state.clear()
        return
    groups = by_course.get(course, [])
    titles = [g.title for g in groups]
    back_courses = [str(c) for c in data.get("courses", [])]
    if await handle_navigation(message, state, titles, "group_page", 10, 2, SetupFlow.course, back_courses):
        return
    selected = next((g for g in groups if g.title == message.text), None)
    if not selected:
        page = data.get("group_page", 0)
        await message.answer("Выбери группу кнопкой.", reply_markup=build_paged_kb(titles, page, 10, 2, True))
        return
    await store.set(message.from_user.id, {
        "group_id": selected.group_id,
        "group_title": selected.title,
        "subdiv_id": subdiv_id,
        "course": course,
    })
    await state.clear()
    await message.answer(f"Ок, группа: {selected.title}", reply_markup=MENU_KB_GROUP)

async def cmd_report(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(ReportFlow.report)
    await state.update_data(report_text="", report_photo=None)
    await message.answer("Опиши проблему. Можно с фото.", reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True
    ))

async def on_report_message(message: Message, state: FSMContext, store: UserSettingsStore, bot: Bot):
    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок", reply_markup=MENU_KB_GROUP)
        return
    data = await state.get_data()
    text = data.get("report_text", "")
    photo = data.get("report_photo")
    new_text = message.caption or message.text or ""
    if new_text and new_text not in ALL_BTNS:
        text = f"{text}\n{new_text}".strip()
    if message.photo:
        photo = message.photo[-1].file_id
    if not text and not photo:
        await message.answer("Нужен текст или фото.")
        return
    await state.update_data(report_text=text, report_photo=photo)
    if text:
        user = message.from_user
        u_line = f"{user.full_name} (@{user.username})" if user else "Unknown"
        settings = await store.get(user.id) if user else {}
        body = html.escape(text[:3000])
        msg = f"<b>Баг-репорт</b>\n<b>User:</b> {html.escape(u_line)}\n<b>Group:</b> {html.escape(str(settings.get('group_title', '-')))}\n{body}"
        try:
            if photo:
                await bot.send_photo(ADMIN_USER_ID, photo, caption=msg[:1024])
            else:
                await bot.send_message(ADMIN_USER_ID, msg)
        except Exception:
            logging.exception("Send report error")
        await state.clear()
        await message.answer("Спасибо! Передал.", reply_markup=MENU_KB_GROUP)
    else:
        await message.answer("Фото принято. Теперь опиши проблему текстом.")

async def on_menu(message: Message, state: FSMContext, schedules: ScheduleClient, store: UserSettingsStore):
    fsm_data = await state.get_data()
    mode = fsm_data.get("mode", "group")
    now = datetime.now(IRKUTSK_TZ)
    if mode == "teacher":
        teacher_id = fsm_data.get("teacher_prep_id")
        if not teacher_id:
            await message.answer("Сначала выбери преподавателя.", reply_markup=MENU_KB_GROUP)
            return
        tid = int(teacher_id)
        if message.text == BTN_TODAY:
            await send_teacher_day(message, schedules, tid, now.date())
        elif message.text == BTN_TOMORROW:
            await send_teacher_day(message, schedules, tid, now.date() + timedelta(days=1))
        elif message.text == BTN_THIS_WEEK:
            await send_teacher_week(message, schedules, tid, now.date() - timedelta(days=now.date().weekday()))
        elif message.text == BTN_NEXT_WEEK:
            await send_teacher_week(message, schedules, tid, now.date() - timedelta(days=now.date().weekday()) + timedelta(days=7))
    else:
        settings = await store.get(message.from_user.id) if message.from_user else {}
        gid = settings.get("group_id")
        if not gid:
            await message.answer("Сначала выбери группу /start", reply_markup=MENU_KB_GROUP)
            return
        gid_int = int(gid)
        if message.text == BTN_TODAY:
            await send_day(message, schedules, gid_int, now.date())
        elif message.text == BTN_TOMORROW:
            await send_day(message, schedules, gid_int, now.date() + timedelta(days=1))
        elif message.text == BTN_THIS_WEEK:
            await send_week(message, schedules, gid_int, now.date() - timedelta(days=now.date().weekday()))
        elif message.text == BTN_NEXT_WEEK:
            await send_week(message, schedules, gid_int, now.date() - timedelta(days=now.date().weekday()) + timedelta(days=7))

async def send_day(message: Message, schedules: ScheduleClient, gid: int, d: date):
    res = await safe_request(message, schedules.get_week_schedule(gid, d))
    if res == "NO_SCHEDULE":
        await message.answer("Нет расписания на этот день.")
        return
    if not res: return
    _, days = res
    target_day = None
    for day in days:
        parsed_date = _parse_date_from_string(day.date_str)
        if parsed_date == d:
            target_day = day
            break
    if target_day:
        await safe_send(message, _format_day_message(target_day.heading, target_day.lessons))
    else:
        await message.answer("Нет расписания на этот день.")

async def send_week(message: Message, schedules: ScheduleClient, gid: int, monday: date):
    res = await safe_request(message, schedules.get_week_schedule(gid, monday))
    if res == "NO_SCHEDULE":
        await message.answer("Нет расписания на неделю.")
        return
    if not res: return
    _, days = res
    week_end = monday + timedelta(days=6)
    picked = []
    seen_dates = set() # Жесткая защита от дубликатов
    for day in days:
        parsed_date = _parse_date_from_string(day.date_str)
        if parsed_date and monday <= parsed_date <= week_end:
            if day.date_str not in seen_dates:
                seen_dates.add(day.date_str)
                picked.append((parsed_date, day))
    picked.sort(key=lambda x: x[0])
    if not picked:
        await message.answer("Нет расписания на неделю.")
        return
    await message.answer("Расписание на неделю:")
    for _, day in picked:
        await safe_send(message, _format_day_message(day.heading, day.lessons))

async def send_teacher_day(message: Message, schedules: ScheduleClient, prep_id: int, d: date):
    res = await safe_request(message, schedules.get_teacher_week_schedule(prep_id, d))
    if res == "NO_SCHEDULE":
        await message.answer("Нет расписания на этот день.")
        return
    if not res: return
    _, days = res
    target_day = None
    for day in days:
        parsed_date = _parse_date_from_string(day.date_str)
        if parsed_date == d:
            target_day = day
            break
    if target_day:
        await safe_send(message, _format_day_message_teacher(target_day.heading, target_day.lessons))
    else:
        await message.answer("Нет расписания на этот день.")

async def send_teacher_week(message: Message, schedules: ScheduleClient, prep_id: int, monday: date):
    res = await safe_request(message, schedules.get_teacher_week_schedule(prep_id, monday))
    if res == "NO_SCHEDULE":
        await message.answer("Нет расписания на неделю.")
        return
    if not res: return
    _, days = res
    week_end = monday + timedelta(days=6)
    picked = []
    seen_dates = set() # Жесткая защита от дубликатов
    for day in days:
        parsed_date = _parse_date_from_string(day.date_str)
        if parsed_date and monday <= parsed_date <= week_end:
            if day.date_str not in seen_dates:
                seen_dates.add(day.date_str)
                picked.append((parsed_date, day))
    picked.sort(key=lambda x: x[0])
    if not picked:
        await message.answer("Нет расписания на неделю.")
        return
    await message.answer("Расписание на неделю:")
    for _, day in picked:
        await safe_send(message, _format_day_message_teacher(day.heading, day.lessons))

async def cmd_teacher_schedule(message: Message, state: FSMContext):
    await state.set_state(TeacherFlow.search)
    await message.answer("Введи ФИО, хотя можно просто фамилию:", reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True
    ))

async def _get_teacher_main_subject(schedules: ScheduleClient, prep_id: int) -> str:
    try:
        today = datetime.now(IRKUTSK_TZ).date()
        monday = today - timedelta(days=today.weekday())
        _, days = await schedules.get_teacher_week_schedule(prep_id, monday)
        subject_count: Dict[str, int] = {}
        for day in days:
            for lesson in day.lessons:
                s = lesson.subject.strip()
                if s:
                    subject_count[s] = subject_count.get(s, 0) + 1
        if not subject_count:
            _, days2 = await schedules.get_teacher_week_schedule(prep_id, monday + timedelta(days=7))
            for day in days2:
                for lesson in day.lessons:
                    s = lesson.subject.strip()
                    if s:
                        subject_count[s] = subject_count.get(s, 0) + 1
        if subject_count:
            return max(subject_count, key=lambda k: subject_count[k])
    except Exception:
        pass
    return ""

async def on_teacher_search(message: Message, state: FSMContext, schedules: ScheduleClient):
    if message.text == BTN_CANCEL:
        fsm_data = await state.get_data()
        if fsm_data.get("teacher_prep_id") and fsm_data.get("mode") == "teacher":
            await state.set_state(None)
            await message.answer("Ок", reply_markup=MENU_KB_TEACHER)
        else:
            await state.set_state(None)
            await message.answer("Ок", reply_markup=MENU_KB_GROUP)
        return
    query = (message.text or "").strip()
    if not query:
        await message.answer("Введи фамилию.")
        return
    await message.answer("Ищу...")
    try:
        teachers = await schedules.search_teachers(query)
    except Exception:
        logging.exception("Teacher search failed")
        await message.answer("⚠️ Не удалось выполнить поиск. Попробуй позже.")
        return
    if not teachers:
        await message.answer("Преподаватель не найден. Попробуй уточнить запрос.", reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True
        ))
        return
    if len(teachers) == 1:
        t = teachers[0]
        main_subject = await _get_teacher_main_subject(schedules, t.prep_id)
        fsm_data = await state.get_data()
        await state.set_data({**fsm_data, "mode": "teacher", "teacher_prep_id": t.prep_id, "teacher_name": t.name})
        await state.set_state(None)
        subject_line = f"\nВедёт: {html.escape(main_subject)}" if main_subject else ""
        await message.answer(f"Преподаватель: {html.escape(t.name)}{subject_line}", reply_markup=MENU_KB_TEACHER)
        return
    fsm_data = await state.get_data()
    await state.set_data({**fsm_data, "_teacher_search_map": {t.name: t.prep_id for t in teachers}})
    await state.set_state(TeacherFlow.select)
    await message.answer(f"Найдено несколько преподавателей ({len(teachers)}). Выбери:", reply_markup=build_teacher_select_kb([t.name for t in teachers]))

async def on_teacher_select(message: Message, state: FSMContext, schedules: ScheduleClient):
    if message.text == BTN_CANCEL:
        fsm_data = await state.get_data()
        clean = {k: v for k, v in fsm_data.items() if k != "_teacher_search_map"}
        await state.set_data(clean)
        if clean.get("teacher_prep_id") and clean.get("mode") == "teacher":
            await state.set_state(None)
            await message.answer("Ок", reply_markup=MENU_KB_TEACHER)
        else:
            await state.set_state(None)
            await message.answer("Ок", reply_markup=MENU_KB_GROUP)
        return
    fsm_data = await state.get_data()
    teacher_map = fsm_data.get("_teacher_search_map", {})
    chosen_name = message.text
    prep_id = teacher_map.get(chosen_name)
    if not prep_id:
        await message.answer("Выбери преподавателя из списка.")
        return
    main_subject = await _get_teacher_main_subject(schedules, prep_id)
    await state.set_data({**{k: v for k, v in fsm_data.items() if k != "_teacher_search_map"}, "mode": "teacher", "teacher_prep_id": prep_id, "teacher_name": chosen_name})
    await state.set_state(None)
    subject_line = f"\nВедёт: {html.escape(main_subject)}" if main_subject else ""
    await message.answer(f"Преподаватель: {html.escape(chosen_name)}{subject_line}", reply_markup=MENU_KB_TEACHER)

async def cmd_switch_to_group(message: Message, state: FSMContext, store: UserSettingsStore):
    fsm_data = await state.get_data()
    await state.set_data({**fsm_data, "mode": "group"})
    await state.set_state(None)
    settings = await store.get(message.from_user.id) if message.from_user else {}
    group_title = settings.get("group_title", "")
    if group_title:
        await message.answer(f"Режим группы: {group_title}", reply_markup=MENU_KB_GROUP)
    else:
        await message.answer("Режим группы. Выбери группу /start", reply_markup=MENU_KB_GROUP)

async def cmd_switch_to_teacher(message: Message, state: FSMContext, schedules: ScheduleClient):
    fsm_data = await state.get_data()
    teacher_id = fsm_data.get("teacher_prep_id")
    teacher_name = fsm_data.get("teacher_name")
    if teacher_id and teacher_name:
        await state.set_data({**fsm_data, "mode": "teacher"})
        await state.set_state(None)
        await message.answer(f"Преподаватель: {html.escape(teacher_name)}", reply_markup=MENU_KB_TEACHER)
    else:
        await cmd_teacher_schedule(message, state)

# ==================== ADMIN HANDLERS ====================
async def _render_admin_menu(message: Message, db: Database):
    test_mode = await get_test_mode(db)
    test_count = len(await get_test_users(db))
    if test_mode:
        mode_line = "🧪 <b>Тестовый режим включён</b>\nРассылка уйдёт только тестовому списку (я по умолчанию)."
    else:
        mode_line = "⚠️ <b>Тестовый режим выключен</b>\nРассылка уйдёт всем пользователям бота."
    await message.answer(
        f"⚙️ <b>Админ-панель</b>\n\n{mode_line}\n\n👥 В тестовом списке: <b>{test_count}</b>",
        reply_markup=build_admin_menu_kb()
    )

async def cmd_admin(message: Message, state: FSMContext, db: Database):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    await state.set_state(AdminPanel.menu)
    await _render_admin_menu(message, db)

async def on_admin_menu_button(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    text = message.text or ""

    if text == BTN_ADM_CLOSE:
        await state.clear()
        await message.answer("Панель закрыта.", reply_markup=MENU_KB_GROUP)
        return

    if text == BTN_ADM_TOGGLE:
        current = await get_test_mode(db)
        await set_test_mode(db, not current)
        await _render_admin_menu(message, db)
        return

    if text == BTN_ADM_TEST_LIST:
        await state.set_state(AdminPanel.test_list)
        users = await get_test_users(db)
        await message.answer("Тестовый список получателей:", reply_markup=build_test_list_kb(users))
        return

    if text == BTN_ADM_BROADCAST:
        if BROADCAST_LOCK.locked():
            await message.answer("⚠️ Рассылка уже выполняется. Дождись её завершения.")
            return

        # Сохраняем маркер текущего процесса. После перезапуска старый
        # сценарий ожидания сообщения будет автоматически аннулирован.
        await _clear_broadcast_data(state)
        await state.update_data(
            broadcast_session_token=BROADCAST_SESSION_TOKEN
        )
        await state.set_state(BroadcastFlow.waiting_text)

        await message.answer(
            "Отправь одно сообщение для рассылки.\n\n"
            "Поддерживаются:\n"
            "• Текст\n"
            "• Фото\n"
            "• Видео\n"
            "• GIF-анимации\n"
            "• Документы\n"
            "• Аудио и голосовые\n"
            "• Видеосообщения (кружки)\n"
            "• Стикеры\n"
            "• Контакты и геолокация\n\n"
            "Альбомы из нескольких файлов пока не поддерживаются.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
                resize_keyboard=True
            )
        )
        return

    await message.answer("Выбери действие кнопкой снизу.")

async def on_test_list_button(message: Message, state: FSMContext, db: Database):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    text = message.text or ""

    if text == BTN_BACK:
        await state.set_state(AdminPanel.menu)
        await _render_admin_menu(message, db)
        return

    if text == BTN_ADM_ADD_ID:
        await state.set_state(AdminPanel.waiting_new_test_id)
        await message.answer(
            "Пришли user_id (число). Можно с подписью через пробел:\n123456789 Рекламодатель",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True)
        )
        return

    if text.startswith("❌ "):
        target = text[2:].strip()
        for uid, label in await get_test_users(db):
            if (label or str(uid)) == target:
                await remove_test_user(db, uid)
                break
        users = await get_test_users(db)
        await message.answer("Обновлено:", reply_markup=build_test_list_kb(users))
        return

    await message.answer("Выбери действие кнопкой снизу.")

async def on_new_test_id(message: Message, state: FSMContext, db: Database):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    if message.text == BTN_CANCEL:
        await state.set_state(AdminPanel.test_list)
        users = await get_test_users(db)
        await message.answer("Добавление отменено.", reply_markup=build_test_list_kb(users))
        return
    parts = (message.text or "").strip().split(maxsplit=1)
    if not parts or not parts[0].lstrip("-").isdigit():
        await message.answer("Не похоже на ID. Пришли число, например: 123456789 Рекламодатель")
        return
    uid = int(parts[0])
    label = parts[1] if len(parts) > 1 else None
    await add_test_user(db, uid, label)
    await state.set_state(AdminPanel.test_list)
    users = await get_test_users(db)
    await message.answer(f"Добавлено: {label or uid}", reply_markup=build_test_list_kb(users))

async def _send_stats(bot: Bot, db: Database, chat_id: int):
    total_row = await db.fetchone("SELECT COUNT(*) FROM registered_users")
    total = total_row[0] if total_row else 0
    await bot.send_message(chat_id, f"<b>📊 Статистика бота</b>\nВсего пользователей: <b>{total}</b>")
    all_rows = await db.fetchall("SELECT ru.user_id, us.group_title, us.course FROM registered_users ru LEFT JOIN user_settings us ON ru.user_id = us.user_id ORDER BY ru.user_id")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "group_name", "course"])
    for row in (all_rows or []):
        writer.writerow([row[0], row[1] if row[1] is not None else "", row[2] if row[2] is not None else ""])
    csv_bytes = output.getvalue().encode("utf-8-sig")
    filename = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    await bot.send_document(chat_id, BufferedInputFile(csv_bytes, filename=filename))

async def cmd_stats(message: Message, bot: Bot, db: Database):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    await _send_stats(bot, db, message.chat.id)

async def _clear_broadcast_data(state: FSMContext):
    """Удаляет только временные данные рассылки, сохраняя остальные данные FSM."""
    data = await state.get_data()
    cleaned = {
        key: value
        for key, value in data.items()
        if not key.startswith("broadcast_")
    }
    await state.set_data(cleaned)


async def _copy_with_retry(
    bot: Bot,
    uid: int,
    from_chat_id: int,
    message_id: int,
    reply_markup=None,
    wait_after_rate_limit: bool = True
) -> str:
    max_attempts = 3

    for attempt in range(max_attempts):
        try:
            await bot.copy_message(
                chat_id=uid,
                from_chat_id=from_chat_id,
                message_id=message_id,
                parse_mode=None,
                reply_markup=reply_markup,
            )
            return "ok"

        except TelegramRetryAfter as e:
            # Ждём даже после последней неудачной попытки, чтобы не
            # отправлять следующему получателю запрос сразу после 429.
            delay = max(0.0, float(e.retry_after)) + 1.0
            is_last_attempt = attempt == max_attempts - 1

            if is_last_attempt:
                if wait_after_rate_limit:
                    logging.warning(
                        f"Broadcast rate limit exhausted for {uid}; "
                        f"waiting {delay:.1f}s before next recipient"
                    )
                    await asyncio.sleep(delay)
                else:
                    logging.warning(
                        f"Broadcast rate limit exhausted for final recipient {uid}; "
                        "no further recipient is waiting"
                    )
                return "rate_limited"

            logging.warning(
                f"Rate limit for {uid}. "
                f"Retry {attempt + 1}/{max_attempts} in {delay:.1f}s"
            )
            await asyncio.sleep(delay)

        except TelegramForbiddenError as e:
            logging.info(f"No access to user {uid}: {e}")
            return "forbidden"

        except (TelegramNetworkError, TelegramServerError) as e:
            if attempt == max_attempts - 1:
                logging.warning(
                    f"Network/server error for {uid} "
                    f"after {max_attempts} attempts: {e}"
                )
                return "error"

            delay = min(RETRY_BASE_DELAY * (2 ** attempt), 10.0)
            logging.warning(
                f"Temporary error for {uid}. "
                f"Retry {attempt + 1}/{max_attempts} "
                f"in {delay:.1f}s: {e}"
            )
            await asyncio.sleep(delay)

        except TelegramAPIError as e:
            logging.warning(f"Telegram API error for {uid}: {e}")
            return "error"

        except Exception:
            logging.exception(f"Unexpected broadcast error for {uid}")
            return "error"

    return "error"


async def on_broadcast_text(
    message: Message,
    state: FSMContext,
    bot: Bot,
    db: Database
):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return

    # Незавершённый сценарий из предыдущего процесса нельзя продолжать.
    data = await state.get_data()
    if data.get("broadcast_session_token") != BROADCAST_SESSION_TOKEN:
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer(
            "⚠️ Предыдущая подготовка рассылки сброшена "
            "после перезапуска бота. Начни заново через админ-панель."
        )
        await _render_admin_menu(message, db)
        return

    if BROADCAST_LOCK.locked():
        await message.answer(
            "⚠️ Рассылка уже выполняется. Дождись её завершения."
        )
        return

    # Отмена до этапа подтверждения.
    if message.text in (BTN_CANCEL, BTN_BROADCAST_CANCEL):
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer("Подготовка рассылки отменена.")
        await _render_admin_menu(message, db)
        return

    # Альбомы пока не поддерживаются: не допускаем отправку каждого
    # элемента альбома как отдельной массовой рассылки.
    if message.media_group_id:
        group_id = message.media_group_id
        if group_id not in _REJECTED_MEDIA_GROUPS:
            _REJECTED_MEDIA_GROUPS.add(group_id)
            await message.answer(
                "⚠️ Альбомы пока не поддерживаются.\n"
                "Отправь одну фотографию или один файл отдельно."
            )
            asyncio.get_running_loop().call_later(
                60,
                _REJECTED_MEDIA_GROUPS.discard,
                group_id
            )
        return

    has_copyable_content = bool(
        (message.text or "").strip()
        or (message.caption or "").strip()
        or message.photo
        or message.video
        or message.animation
        or message.document
        or message.audio
        or message.voice
        or message.video_note
        or message.sticker
        or message.contact
        or message.location
        or message.venue
        or message.dice
    )

    if not has_copyable_content:
        await message.answer(
            "Не удалось определить поддерживаемый тип сообщения.\n"
            "Пришли текст, фото, видео, документ или другой "
            "поддерживаемый тип сообщения."
        )
        return

    # Фиксируем режим и точный список получателей на момент подготовки.
    test_mode = await get_test_mode(db)

    if test_mode:
        test_users = await get_test_users(db)
        user_ids = [uid for uid, _ in test_users]
        if ADMIN_USER_ID not in user_ids:
            user_ids.append(ADMIN_USER_ID)
        mode_note = "🧪 ТЕСТОВЫЙ режим"
    else:
        rows = await db.fetchall(
            "SELECT user_id FROM registered_users WHERE user_id != %s",
            (ADMIN_USER_ID,)
        )
        user_ids = [row[0] for row in rows] if rows else []
        mode_note = "⚠️ РАССЫЛКА ВСЕМ"

    # Убираем возможные дубликаты и нормализуем Telegram ID.
    user_ids = list(dict.fromkeys(int(uid) for uid in user_ids))

    if not user_ids:
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer("Список получателей пуст. Рассылка не запущена.")
        await _render_admin_menu(message, db)
        return

    # Храним в FSM исходное сообщение и неизменяемый снимок получателей.
    # Это исключает изменение аудитории между предпросмотром и подтверждением.
    await state.update_data(
        broadcast_session_token=BROADCAST_SESSION_TOKEN,
        broadcast_created_at=datetime.now(timezone.utc).timestamp(),
        broadcast_chat_id=message.chat.id,
        broadcast_message_id=message.message_id,
        broadcast_user_ids=user_ids,
        broadcast_mode_note=mode_note,
    )
    await state.set_state(BroadcastFlow.confirm)

    confirm_keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_BROADCAST_CONFIRM)],
            [KeyboardButton(text=BTN_BROADCAST_CANCEL)],
        ],
        resize_keyboard=True
    )

    await message.answer(
        f"Проверь рассылку:\n\n"
        f"Режим: {mode_note}\n"
        f"Получателей: {len(user_ids)}\n\n"
        f"Отправить это сообщение указанным получателям?",
        reply_markup=confirm_keyboard
    )


async def on_broadcast_confirm(
    message: Message,
    state: FSMContext,
    bot: Bot,
    db: Database
):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return

    # Пока рассылка выполняется, повторное подтверждение не запускает её снова.
    if BROADCAST_LOCK.locked():
        await message.answer("⏳ Рассылка уже выполняется. Дождись итогового отчёта.")
        return

    data = await state.get_data()

    # Маркер создаётся при запуске процесса. После рестарта он меняется,
    # поэтому старое подтверждение не может запустить массовую отправку.
    if data.get("broadcast_session_token") != BROADCAST_SESSION_TOKEN:
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer(
            "⚠️ Подтверждение рассылки аннулировано после перезапуска бота. "
            "Подготовь рассылку заново."
        )
        await _render_admin_menu(message, db)
        return

    created_at = data.get("broadcast_created_at")
    try:
        age_seconds = datetime.now(timezone.utc).timestamp() - float(created_at)
    except (TypeError, ValueError):
        age_seconds = BROADCAST_CONFIRM_TTL_SECONDS + 1

    if age_seconds < 0 or age_seconds > BROADCAST_CONFIRM_TTL_SECONDS:
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer(
            "⌛ Время подтверждения истекло (10 минут). "
            "Подготовь рассылку заново."
        )
        await _render_admin_menu(message, db)
        return

    if message.text in (BTN_BROADCAST_CANCEL, BTN_CANCEL):
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer("Рассылка отменена. Ничего не отправлено.")
        await _render_admin_menu(message, db)
        return

    if message.text != BTN_BROADCAST_CONFIRM:
        await message.answer(
            "Рассылка пока не отправлена. Нажми «✅ Да, отправить» "
            "для подтверждения или «❌ Нет, отменить» для отмены."
        )
        return

    # Проверяем сохранённые параметры перед отправкой.
    try:
        from_chat_id = int(data["broadcast_chat_id"])
        source_message_id = int(data["broadcast_message_id"])
        user_ids = list(dict.fromkeys(
            int(uid) for uid in data["broadcast_user_ids"]
        ))
        mode_note = str(data["broadcast_mode_note"])
    except (KeyError, TypeError, ValueError):
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer(
            "⚠️ Не удалось восстановить параметры рассылки. "
            "Ничего не отправлено. Подготовь её заново."
        )
        await _render_admin_menu(message, db)
        return

    if not user_ids:
        await _clear_broadcast_data(state)
        await state.set_state(AdminPanel.menu)
        await message.answer("Список получателей пуст. Рассылка не запущена.")
        await _render_admin_menu(message, db)
        return

    # Проверка и захват блокировки выполняются без промежуточного await.
    if BROADCAST_LOCK.locked():
        await message.answer("⚠️ Другая рассылка уже выполняется.")
        return
    await BROADCAST_LOCK.acquire()

    sent = 0
    forbidden = 0
    rate_limited = 0
    failed = 0

    try:
        await message.answer(
            f"🚀 Подтверждение принято. Начинаю рассылку ({mode_note}).\n"
            f"Получателей: {len(user_ids)}"
        )

        for index, uid in enumerate(user_ids):
            result = await _copy_with_retry(
                bot,
                uid,
                from_chat_id,
                source_message_id,
                wait_after_rate_limit=(index < len(user_ids) - 1),
            )

            if result == "ok":
                sent += 1
            elif result == "forbidden":
                forbidden += 1
            elif result == "rate_limited":
                rate_limited += 1
            else:
                failed += 1

            await asyncio.sleep(0.05)

        await message.answer(
            f"✅ Рассылка завершена ({mode_note}).\n\n"
            f"Доставлено: {sent}\n"
            f"Нет доступа (403): {forbidden}\n"
            f"Лимит Telegram (429): {rate_limited}\n"
            f"Прочие ошибки: {failed}\n"
            f"Всего получателей: {len(user_ids)}"
        )

    except Exception:
        logging.exception("Broadcast process failed")
        try:
            await message.answer(
                "⚠️ Рассылка была прервана из-за внутренней ошибки. "
                "Проверь логи сервера и статистику доставки."
            )
        except Exception:
            logging.exception("Failed to send broadcast interruption notice")

    finally:
        try:
            await _clear_broadcast_data(state)
            await state.set_state(AdminPanel.menu)
        finally:
            BROADCAST_LOCK.release()

    await _render_admin_menu(message, db)


# ==================== FSM CLEANUP TASK ====================
async def fsm_cleanup_task(fsm_storage: MySQLStorage):
    while True:
        await asyncio.sleep(FSM_CLEANUP_INTERVAL)
        try:
            await fsm_storage.cleanup()
        except Exception:
            logging.exception("FSM cleanup error")

# ==================== MAIN ====================
async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN required")

    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML), session=AiohttpSession(proxy=os.getenv("TELEGRAM_PROXY")))
    db = Database(os.getenv("DB_HOST", "localhost"), int(os.getenv("DB_PORT", "3306")), os.getenv("DB_USER", "istu_bot"), os.getenv("DB_PASSWORD", ""), os.getenv("DB_NAME", "istu_bot"))
    await db.connect()
    store = UserSettingsStore(db)
    await store.initialize()
    await init_registered_users_table(db)
    await init_broadcast_test_table(db)
    await import_users_from_file(db, USER_IDS_FILE)
    fsm_storage = MySQLStorage(db)
    await fsm_storage.initialize()

    dp = Dispatcher(storage=fsm_storage)

    async with aiohttp.ClientSession(headers={"User-Agent": "ISTU-Bot/2.3"}) as http:
        schedules = ScheduleClient(http)
        ref_cache = ReferenceDataCache(schedules)

        dp["store"] = store
        dp["schedules"] = schedules
        dp["ref_cache"] = ref_cache
        dp["db"] = db

        dp.message.register(cmd_start, Command("start"))
        dp.message.register(cmd_start, F.text == BTN_CHANGE_GROUP)
        dp.message.register(cmd_report, F.text == BTN_REPORT)
        dp.message.register(on_setup_institute, SetupFlow.institute)
        dp.message.register(on_setup_course, SetupFlow.course)
        dp.message.register(on_setup_group, SetupFlow.group)
        dp.message.register(on_report_message, ReportFlow.report)
        dp.message.register(on_menu, F.text.in_({BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK}))
        dp.message.register(cmd_switch_to_teacher, F.text == BTN_TEACHER_SCHEDULE)
        dp.message.register(cmd_teacher_schedule, F.text == BTN_CHANGE_TEACHER)
        dp.message.register(cmd_switch_to_group, F.text == BTN_GROUP_SCHEDULE)
        dp.message.register(on_teacher_search, TeacherFlow.search)
        dp.message.register(on_teacher_select, TeacherFlow.select)
        dp.message.register(cmd_admin, Command("admin"))
        dp.message.register(cmd_stats, Command("stats"))
        dp.message.register(on_admin_menu_button, AdminPanel.menu)
        dp.message.register(on_test_list_button, AdminPanel.test_list)
        dp.message.register(on_new_test_id, AdminPanel.waiting_new_test_id)
        dp.message.register(on_broadcast_text, BroadcastFlow.waiting_text)
        dp.message.register(on_broadcast_confirm, BroadcastFlow.confirm)

        cleanup_task = asyncio.create_task(fsm_cleanup_task(fsm_storage))

        try:
            await bot.delete_webhook(drop_pending_updates=True)
            me = await bot.get_me()
            logging.info(f"Bot @{me.username} started")
            await bot.set_my_commands([BotCommand(command="start", description="Запустить бота")])
            try:
                await bot.set_my_commands([
                    BotCommand(command="start", description="Запустить бота"),
                    BotCommand(command="admin", description="⚙️ Админ-панель"),
                    BotCommand(command="stats", description="📊 Статистика + CSV"),
                ], scope=BotCommandScopeChat(chat_id=ADMIN_USER_ID))
            except Exception:
                logging.warning("Не удалось установить команды администратора — продолжаю")
            await dp.start_polling(bot)
        finally:
            cleanup_task.cancel()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
            await db.disconnect()
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
