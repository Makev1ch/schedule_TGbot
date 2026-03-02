#!/usr/bin/env python3
import asyncio
import csv
import io
import logging
import json
import html
import os
import re
import random
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
from aiogram.exceptions import TelegramNetworkError
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

BASE_SCHEDULE_URL = "https://www.istu.edu/schedule/"
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID") or "1307617601")

# Path to the file with user IDs for initial import
USER_IDS_FILE = Path(__file__).with_name("telegram_ids.txt")

# Network Settings
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
REQUEST_TIMEOUT = 15.0

# FSM cleanup interval
FSM_CLEANUP_INTERVAL = 3 * 60 * 60  # 3 часа

# ==================== REGEX ====================
RE_TIME = re.compile(r"^(\d{1,2}):(\d{2})$")
RE_SUBGROUP = re.compile(r"подгруппа\s*(\d+)", re.IGNORECASE)
RE_KIND_LECTURE = re.compile(r"лекц", re.IGNORECASE)
RE_KIND_PRACTICE = re.compile(r"практ", re.IGNORECASE)
RE_KIND_LAB = re.compile(r"лаб(?:оратор)?", re.IGNORECASE)
RE_DAY_MONTH = re.compile(r",\s*(\d{1,2})\s+([а-яё]+)", re.IGNORECASE)
RE_SUBDIV_ID = re.compile(r"^\?subdiv=(\d+)$")
RE_GROUP_ID = re.compile(r"^\?group=(\d+)$")
RE_COURSE_ID = re.compile(r"^Курс\s*(\d+)\b", re.IGNORECASE)
RE_PREP_ID = re.compile(r"^\?prep=(\d+)$")  # [ADDED] для парсинга prep_id преподавателей

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
BTN_PAGE_PREV = "⬅️"
BTN_PAGE_NEXT = "➡️"
BTN_CANCEL = "❌ Отмена"

# [ADDED] Кнопки переключения режимов
BTN_TEACHER_SCHEDULE = "👨‍🏫 Расписание преподавателей"
BTN_GROUP_SCHEDULE = "👥 Расписание группы"
BTN_CHANGE_TEACHER = "🔁 Сменить преподавателя"  # [ADDED] замена BTN_CHANGE_GROUP в меню препода

ALL_BTNS = {BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK,
            BTN_CHANGE_GROUP, BTN_REPORT, BTN_BACK, BTN_PAGE_PREV, BTN_PAGE_NEXT, BTN_CANCEL,
            BTN_TEACHER_SCHEDULE, BTN_GROUP_SCHEDULE, BTN_CHANGE_TEACHER}  # [ADDED] новые кнопки

# [UNCHANGED] Оригинальное MENU_KB не меняется
MENU_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_GROUP), KeyboardButton(text=BTN_REPORT)],
    ],
    resize_keyboard=True,
)

# [ADDED] Меню группы — как MENU_KB + кнопка преподавателей внизу
MENU_KB_GROUP = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_GROUP), KeyboardButton(text=BTN_REPORT)],
        [KeyboardButton(text=BTN_TEACHER_SCHEDULE)],
    ],
    resize_keyboard=True,
)

# [ADDED] Меню преподавателя — идентично MENU_KB_GROUP, но 3я и 4я строки другие
MENU_KB_TEACHER = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_TEACHER), KeyboardButton(text=BTN_REPORT)],
        [KeyboardButton(text=BTN_GROUP_SCHEDULE)],
    ],
    resize_keyboard=True,
)

# ==================== FSM ====================
class SetupFlow(StatesGroup):
    institute = State()
    course = State()
    group = State()

class ReportFlow(StatesGroup):
    report = State()

# [ADDED] FSM для поиска преподавателя
class TeacherFlow(StatesGroup):
    search = State()   # ожидание ввода ФИО
    select = State()   # ожидание выбора из списка

# [ADDED] FSM для рассылки
class BroadcastFlow(StatesGroup):
    waiting_text = State()

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
        self.group_name = group_name  # [ADDED] для расписания преподавателя

class DaySchedule:
    __slots__ = ('heading', 'lessons')
    def __init__(self, heading: str, lessons: List[Lesson]):
        self.heading = heading
        self.lessons = lessons

# [ADDED] Модель преподавателя
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

def _extract_date_from_heading(heading: str, reference: date) -> Optional[date]:
    m = RE_DAY_MONTH.search(heading.lower())
    if not m:
        return None
    day, month_name = int(m.group(1)), m.group(2)
    month = RU_MONTHS.get(month_name)
    if not month:
        return None
    year = reference.year
    if reference.month == 12 and month == 1:
        year += 1
    elif reference.month == 1 and month == 12:
        year -= 1
    try:
        return date(year, month, day)
    except ValueError:
        return None

def _format_day_message(heading: str, lessons: List[Lesson]) -> str:
    sep = "-------------------------"
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
            kind = f" ({html.escape(lesson.kind)})" if lesson.kind else ""
            out.append(
                f"{start_t.strftime('%H:%M')} — {end_dt.time().strftime('%H:%M')} "
                f"{html.escape(lesson.subject)}{kind}"
            )
            
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

# [ADDED] Форматирование расписания для режима преподавателя:
# показывает группу вместо имени преподавателя
def _format_day_message_teacher(heading: str, lessons: List[Lesson]) -> str:
    sep = "-------------------------"
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
            kind = f" ({html.escape(lesson.kind)})" if lesson.kind else ""
            out.append(
                f"{start_t.strftime('%H:%M')} — {end_dt.time().strftime('%H:%M')} "
                f"{html.escape(lesson.subject)}{kind}"
            )
            
            # Для препода: показываем группу + подгруппу + аудиторию (без имени препода)
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

# [ADDED] Клавиатура для выбора преподавателя из списка
def build_teacher_select_kb(teacher_names: List[str]) -> ReplyKeyboardMarkup:
    keyboard: List[List[KeyboardButton]] = []
    for name in teacher_names:
        keyboard.append([KeyboardButton(text=name)])
    keyboard.append([KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# ==================== REFERENCE DATA CACHE ====================
class ReferenceDataCache:
    """
    In-memory кэш для справочных данных (институты, группы).
    Вместо хранения в FSM на каждого юзера - один экземпляр в памяти бота.
    """
    
    def __init__(self, schedule_client: 'ScheduleClient'):
        self._client = schedule_client
        
        # Кэш институтов
        self._institutes: Optional[List[Institute]] = None
        self._institutes_loaded_at: Optional[float] = None
        self._institutes_ttl = 1800.0  # 30 минут
        
        # Кэш групп: {subdiv_id: {course: [Group]}}
        self._groups_cache: Dict[int, Dict[int, List[Group]]] = {}
        self._groups_loaded_at: Dict[int, float] = {}
        self._groups_ttl = 600.0  # 10 минут
        
        # Индексы для быстрого поиска
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
            loaded_at = self._groups_loaded_at.get(subdiv_id, 0)
            if now - loaded_at < self._groups_ttl:
                return self._groups_cache[subdiv_id]
        
        by_course = await self._client.list_groups_by_course(subdiv_id)
        self._groups_cache[subdiv_id] = by_course
        self._groups_loaded_at[subdiv_id] = now
        
        return by_course
    
    def get_cached_groups(self, subdiv_id: int) -> Optional[Dict[int, List[Group]]]:
        return self._groups_cache.get(subdiv_id)

# ==================== NAVIGATION LOGIC ====================
async def handle_navigation(
    message: Message,
    state: FSMContext,
    options: List[str],
    page_key: str,
    page_size: int,
    row_size: int,
    back_state: Optional[State] = None,
    back_options: Optional[List[str]] = None
) -> bool:
    text = message.text
    
    if text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок", reply_markup=MENU_KB_GROUP)
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

async def safe_send(message: Message, text: str, limit: int = 3800):
    while text:
        await message.answer(text[:limit])
        text = text[limit:]

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
    
    async def _fetch(self, url: str, params: dict) -> str:
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                async with self._session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    if resp.status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info,
                            resp.history,
                            status=resp.status,
                            message="Server Error"
                        )
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
        content = soup.select_one("div.content")
        if not content:
            raise RuntimeError("Invalid HTML structure")
        
        inst = []
        for a in content.select('a[href^="?subdiv="]'):
            m = RE_SUBDIV_ID.match(a.get("href") or "")
            if m:
                title = a.get_text(" ", strip=True)
                inst.append(Institute(int(m.group(1)), title))
        
        inst.sort(key=lambda x: x.title.lower())
        return inst
    
    async def list_groups_by_course(self, subdiv_id: int) -> dict[int, List[Group]]:
        html_content = await self._fetch(BASE_SCHEDULE_URL, {"subdiv": subdiv_id})
        soup = BeautifulSoup(html_content, "html.parser")
        # Используем div.content если есть, иначе весь документ
        content = soup.select_one("div.content") or soup
        
        by_course: dict[int, List[Group]] = {}
        
        # --- Стратегия 1: <h3>Курс N</h3> + <ul> (оригинальная структура) ---
        for h in content.select("h3"):
            text = h.get_text(" ", strip=True)
            m = RE_COURSE_ID.match(text)
            if not m:
                continue
            ul = h.find_next_sibling("ul")
            if not ul:
                continue
            course = int(m.group(1))
            groups = []
            for a in ul.select('a[href^="?group="]'):
                mg = RE_GROUP_ID.match(a.get("href") or "")
                if mg:
                    title = a.get_text(" ", strip=True)
                    groups.append(Group(int(mg.group(1)), title))
            if groups:
                groups.sort(key=lambda g: g.title.lower())
                by_course[course] = groups
        
        # --- Стратегия 2 (fallback): обход всех элементов ---
        # Срабатывает если сайт изменил структуру (нет h3/ul, используется li/p и т.д.)
        if not by_course:
            logging.warning(f"[subdiv={subdiv_id}] h3+ul parser дал 0 курсов, пробую fallback")
            current_course: Optional[int] = None
            for el in content.descendants:
                tag = getattr(el, 'name', None)
                if not tag:
                    continue
                # Ищем заголовок курса в любом теге
                if tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'p', 'b', 'strong', 'li', 'span'):
                    text = el.get_text(" ", strip=True)
                    m = RE_COURSE_ID.match(text)
                    if m:
                        current_course = int(m.group(1))
                        if current_course not in by_course:
                            by_course[current_course] = []
                # Ищем ссылки на группы
                elif tag == 'a' and current_course is not None:
                    href = el.get('href', '')
                    mg = RE_GROUP_ID.match(href)
                    if mg:
                        title = el.get_text(" ", strip=True)
                        if title:
                            by_course[current_course].append(Group(int(mg.group(1)), title))
            
            # Убираем дубли и сортируем
            for course in list(by_course.keys()):
                seen_ids: set = set()
                unique = []
                for g in by_course[course]:
                    if g.group_id not in seen_ids:
                        seen_ids.add(g.group_id)
                        unique.append(g)
                if unique:
                    unique.sort(key=lambda g: g.title.lower())
                    by_course[course] = unique
                else:
                    del by_course[course]
            
            logging.info(f"[subdiv={subdiv_id}] Fallback нашёл курсов: {list(by_course.keys())}")
        
        return by_course
    
    async def get_week_schedule(self, group_id: int, target_date: date) -> tuple[bool, List[DaySchedule]]:
        self._prune()
        y, w = iso_week_key(target_date)
        key = f"{group_id}:{y}:{w}"
        
        if cached := self._cache.get(key):
            return cached[1]
        
        html_content = await self._fetch(
            BASE_SCHEDULE_URL,
            {"group": group_id, "date": target_date.strftime("%Y-%m-%d")}
        )
        
        result = self._parse_schedule_html(html_content, target_date)
        self._cache[key] = (asyncio.get_running_loop().time(), result)
        return result

    # [ADDED] Расписание преподавателя
    async def get_teacher_week_schedule(self, prep_id: int, target_date: date) -> tuple[bool, List[DaySchedule]]:
        self._prune()
        y, w = iso_week_key(target_date)
        key = f"prep:{prep_id}:{y}:{w}"
        
        if cached := self._cache.get(key):
            return cached[1]
        
        html_content = await self._fetch(
            BASE_SCHEDULE_URL,
            {"prep": prep_id, "date": target_date.strftime("%Y-%m-%d")}
        )
        
        result = self._parse_schedule_html(html_content, target_date)
        self._cache[key] = (asyncio.get_running_loop().time(), result)
        return result

    # [ADDED] Вынесена логика парсинга расписания (используется и для группы, и для препода)
    def _parse_schedule_html(self, html_content: str, target_date: date) -> tuple[bool, List[DaySchedule]]:
        soup = BeautifulSoup(html_content, "html.parser")
        content = soup.select_one("div.content")
        if not content:
            raise RuntimeError("Invalid HTML structure")
        
        odd = is_odd_week(target_date)
        days: List[DaySchedule] = []
        
        for h in content.select("h3.day-heading"):
            heading = h.get_text(" ", strip=True)
            lines = h.find_next_sibling("div", class_="class-lines")
            if not lines:
                continue
            
            lessons: List[Lesson] = []
            for item in lines.select("div.class-line-item"):
                time_el = item.select_one("div.class-time")
                start = _parse_time(time_el.get_text(strip=True)) if time_el else None
                if not start:
                    continue
                
                tails = list(item.select("div.class-tail.class-all-week"))
                parity_class = "class-odd-week" if odd else "class-even-week"
                tails.extend(item.select(f"div.class-tail.{parity_class}"))
                
                for tail in tails:
                    if tail.get_text(" ", strip=True).lower() == "свободно":
                        continue
                    
                    for subject_el in tail.select("div.class-pred"):
                        subject = subject_el.get_text(" ", strip=True)
                        if not subject:
                            continue
                        
                        segment = []
                        sib = subject_el
                        while True:
                            sib = sib.find_next_sibling()
                            if not sib:
                                break
                            cls = sib.get("class") or []
                            if "class-pred" in cls:
                                break
                            segment.append(sib)

                        kind_info = subject_el.find_previous_sibling("div", class_="class-info")
                        if not kind_info:
                            kind_info = next((n for n in segment if "class-info" in (n.get("class") or [])), None)
                        kind = _extract_lesson_kind(kind_info.get_text(" ", strip=True)) if kind_info else ""

                        details_info = next((n for n in segment if "class-info" in (n.get("class") or [])), kind_info)

                        subgroup_source = details_info.get_text(" ", strip=True) if details_info else " ".join(
                            n.get_text(" ", strip=True) for n in segment
                        )
                        subgroup = _extract_subgroup(subgroup_source)

                        teacher = "—"
                        teacher_links = kind_info.select('a[href^="?prep="]') if kind_info else []
                        if not teacher_links:
                            for n in segment:
                                teacher_links.extend(n.select('a[href^="?prep="]'))
                        if teacher_links:
                            seen = set()
                            teacher_names = []
                            for a in teacher_links:
                                name = a.get_text(" ", strip=True)
                                if name and name not in seen:
                                    seen.add(name)
                                    teacher_names.append(name)
                            if teacher_names:
                                teacher = ", ".join(teacher_names)

                        aud_el = next((n for n in segment if "class-aud" in (n.get("class") or [])), None)
                        if not aud_el and details_info:
                            aud_el = details_info.find_next_sibling("div", class_="class-aud")
                        room = aud_el.get_text(" ", strip=True) if aud_el else "—"

                        # [ADDED] Извлекаем группу из ссылок ?group= (нужно для расписания препода)
                        group_name = ""
                        group_links = []
                        if kind_info:
                            group_links = kind_info.select('a[href^="?group="]')
                        if not group_links:
                            for n in segment:
                                group_links.extend(n.select('a[href^="?group="]'))
                        if group_links:
                            seen_g = set()
                            group_names = []
                            for a in group_links:
                                gname = a.get_text(" ", strip=True)
                                if gname and gname not in seen_g:
                                    seen_g.add(gname)
                                    group_names.append(gname)
                            group_name = ", ".join(group_names)

                        lessons.append(Lesson(start, subject, kind, subgroup, room or "—", teacher or "—", group_name))
            
            days.append(DaySchedule(heading, lessons))
        
        return (odd, days)

    # [ADDED] Поиск преподавателей
    async def search_teachers(self, query: str) -> List[Teacher]:
        """
        Ищет преподавателей на https://www.istu.edu/schedule/?search=y&q=QUERY
        Возвращает список Teacher (prep_id, name).
        Хрупкий парсинг допустим по условию задачи.
        """
        # URL-формат: https://www.istu.edu/schedule/?search=ФАМИЛИЯ
        html_content = await self._fetch(
            BASE_SCHEDULE_URL,
            {"search": query}
        )
        soup = BeautifulSoup(html_content, "html.parser")
        
        teachers: List[Teacher] = []
        seen_ids: set = set()
        
        # Ищем все ссылки с ?prep=ID
        for a in soup.select('a[href^="?prep="]'):
            m = RE_PREP_ID.match(a.get("href") or "")
            if not m:
                continue
            prep_id = int(m.group(1))
            if prep_id in seen_ids:
                continue
            seen_ids.add(prep_id)
            # Соединяем текстовые узлы без разделителя, чтобы избежать
            # разрыва имён вида <b>Иванов</b>на → "Иванов на"
            name = "".join(a.strings).strip()
            name = re.sub(r"\s+", " ", name)  # нормализуем пробелы между словами
            if name:
                teachers.append(Teacher(prep_id, name))
        
        return teachers


# ==================== DATABASE EXTENSIONS ====================

# [ADDED] Инициализация таблицы зарегистрированных пользователей (для рассылки)
async def init_registered_users_table(db: Database):
    await db.execute("""
        CREATE TABLE IF NOT EXISTS registered_users (
            user_id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    logger = logging.getLogger(__name__)
    logger.info("registered_users table initialized")

# [ADDED] Добавить user_id в registered_users
async def register_user(db: Database, user_id: int):
    try:
        await db.execute(
            "INSERT IGNORE INTO registered_users (user_id) VALUES (%s)",
            (user_id,)
        )
    except Exception:
        logging.getLogger(__name__).exception(f"Failed to register user {user_id}")

# [ADDED] Импорт user_id из txt-файла в registered_users
async def import_users_from_file(db: Database, filepath: Path):
    if not filepath.exists():
        logging.getLogger(__name__).warning(f"User IDs file not found: {filepath}")
        return
    
    count = 0
    errors = 0
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                uid = int(line)
                await db.execute(
                    "INSERT IGNORE INTO registered_users (user_id) VALUES (%s)",
                    (uid,)
                )
                count += 1
            except Exception:
                errors += 1
    
    logging.getLogger(__name__).info(f"Imported {count} user IDs from file ({errors} errors)")


# ==================== HANDLERS ====================
async def safe_request(message: Message, coro):
    try:
        return await coro
    except Exception as e:
        logging.exception("Schedule request failed")
        await message.answer("⚠️ Не удалось связаться с сервером ИРНИТУ. Попробуй позже.")
        return None

async def cmd_start(message: Message, state: FSMContext, ref_cache: ReferenceDataCache, db: Database):
    # [EXTENDED] Регистрируем пользователя при старте
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
    # [EXTENDED] Показываем меню группы с кнопкой преподавателей
    await message.answer(f"Ок, группа: {selected.title}", reply_markup=MENU_KB_GROUP)

async def cmd_report(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(ReportFlow.report)
    await state.update_data(report_text="", report_photo=None)
    await message.answer(
        "Опиши проблему. Можно с фото.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
            resize_keyboard=True
        )
    )

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
        msg = (
            f"<b>Баг-репорт</b>\n"
            f"<b>User:</b> {html.escape(u_line)}\n"
            f"<b>Group:</b> {html.escape(str(settings.get('group_title', '-')))}\n\n"
            f"{body}"
        )
        
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
    # [EXTENDED] Проверяем режим (group / teacher) из FSM данных
    fsm_data = await state.get_data()
    mode = fsm_data.get("mode", "group")
    
    now = datetime.now(IRKUTSK_TZ)
    
    if mode == "teacher":
        # Режим преподавателя
        teacher_id = fsm_data.get("teacher_prep_id")
        teacher_name = fsm_data.get("teacher_name", "Преподаватель")
        if not teacher_id:
            await message.answer("Сначала выбери преподавателя.", reply_markup=MENU_KB_GROUP)
            return
        
        if message.text == BTN_TODAY:
            await send_teacher_day(message, schedules, int(teacher_id), now.date())
        elif message.text == BTN_TOMORROW:
            await send_teacher_day(message, schedules, int(teacher_id), now.date() + timedelta(days=1))
        elif message.text == BTN_THIS_WEEK:
            await send_teacher_week(message, schedules, int(teacher_id), now.date() - timedelta(days=now.date().weekday()))
        elif message.text == BTN_NEXT_WEEK:
            await send_teacher_week(message, schedules, int(teacher_id), now.date() - timedelta(days=now.date().weekday()) + timedelta(days=7))
    else:
        # Режим группы (оригинальная логика)
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
    if not res:
        return
    _, days = res
    day = next((x for x in days if _extract_date_from_heading(x.heading, d) == d), None)
    if day:
        await safe_send(message, _format_day_message(day.heading, day.lessons))
    else:
        await message.answer("Нет расписания на этот день.")

async def send_week(message: Message, schedules: ScheduleClient, gid: int, monday: date):
    res = await safe_request(message, schedules.get_week_schedule(gid, monday))
    if not res:
        return
    _, days = res
    week_end = monday + timedelta(days=6)
    
    picked = []
    for d in days:
        dd = _extract_date_from_heading(d.heading, monday)
        if dd and monday <= dd <= week_end:
            picked.append((dd, d))
    
    picked.sort(key=lambda x: x[0])
    
    if not picked:
        await message.answer("Нет расписания на неделю.")
        return
    
    await message.answer("Расписание на неделю:")
    for _, d in picked:
        await safe_send(message, _format_day_message(d.heading, d.lessons))


# [ADDED] Расписание преподавателя: день
async def send_teacher_day(message: Message, schedules: ScheduleClient, prep_id: int, d: date):
    res = await safe_request(message, schedules.get_teacher_week_schedule(prep_id, d))
    if not res:
        return
    _, days = res
    day = next((x for x in days if _extract_date_from_heading(x.heading, d) == d), None)
    if day:
        await safe_send(message, _format_day_message_teacher(day.heading, day.lessons))
    else:
        await message.answer("Нет расписания на этот день.")

# [ADDED] Расписание преподавателя: неделя
async def send_teacher_week(message: Message, schedules: ScheduleClient, prep_id: int, monday: date):
    res = await safe_request(message, schedules.get_teacher_week_schedule(prep_id, monday))
    if not res:
        return
    _, days = res
    week_end = monday + timedelta(days=6)
    
    picked = []
    for d in days:
        dd = _extract_date_from_heading(d.heading, monday)
        if dd and monday <= dd <= week_end:
            picked.append((dd, d))
    
    picked.sort(key=lambda x: x[0])
    
    if not picked:
        await message.answer("Нет расписания на неделю.")
        return
    
    await message.answer("Расписание на неделю:")
    for _, d in picked:
        await safe_send(message, _format_day_message_teacher(d.heading, d.lessons))


# [ADDED] Обработчик кнопки «Расписание преподавателей» и «Сменить преподавателя»
async def cmd_teacher_schedule(message: Message, state: FSMContext):
    await state.set_state(TeacherFlow.search)
    await message.answer(
        "Введи ФИО, хотя можно просто фамилию:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
            resize_keyboard=True
        )
    )

# [ADDED] Вспомогательная: определяем основной предмет преподавателя по расписанию
async def _get_teacher_main_subject(schedules: ScheduleClient, prep_id: int) -> str:
    """Загружаем текущую неделю и считаем самый частый предмет."""
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
            # Пробуем следующую неделю
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

# [ADDED] Обработка ввода ФИО в TeacherFlow.search
async def on_teacher_search(message: Message, state: FSMContext, schedules: ScheduleClient):
    if message.text == BTN_CANCEL:
        fsm_data = await state.get_data()
        # Если уже был выбран преподаватель — возвращаем в его меню
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
        await message.answer(
            "Преподаватель не найден. Попробуй уточнить запрос.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
                resize_keyboard=True
            )
        )
        return
    
    if len(teachers) == 1:
        t = teachers[0]
        main_subject = await _get_teacher_main_subject(schedules, t.prep_id)
        fsm_data = await state.get_data()
        await state.set_data({
            **fsm_data,
            "mode": "teacher",
            "teacher_prep_id": t.prep_id,
            "teacher_name": t.name,
        })
        await state.set_state(None)
        subject_line = f"\nВедёт: {html.escape(main_subject)}" if main_subject else ""
        await message.answer(
            f"Преподаватель: {html.escape(t.name)}{subject_line}",
            reply_markup=MENU_KB_TEACHER
        )
        return
    
    # Несколько результатов — предлагаем выбрать
    fsm_data = await state.get_data()
    await state.set_data({
        **fsm_data,
        "_teacher_search_map": {t.name: t.prep_id for t in teachers},
    })
    await state.set_state(TeacherFlow.select)
    
    await message.answer(
        f"Найдено несколько преподавателей ({len(teachers)}). Выбери:",
        reply_markup=build_teacher_select_kb([t.name for t in teachers])
    )

# [ADDED] Обработка выбора преподавателя из списка
async def on_teacher_select(message: Message, state: FSMContext, schedules: ScheduleClient):
    if message.text == BTN_CANCEL:
        fsm_data = await state.get_data()
        # Очищаем временный search map но сохраняем всё остальное
        clean = {k: v for k, v in fsm_data.items() if k != "_teacher_search_map"}
        await state.set_data(clean)
        # Если уже был преподаватель — возвращаем в его меню, иначе в групповое
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
    
    await state.set_data({
        **{k: v for k, v in fsm_data.items() if k != "_teacher_search_map"},
        "mode": "teacher",
        "teacher_prep_id": prep_id,
        "teacher_name": chosen_name,
    })
    await state.set_state(None)
    subject_line = f"\nВедёт: {html.escape(main_subject)}" if main_subject else ""
    await message.answer(
        f"Преподаватель: {html.escape(chosen_name)}{subject_line}",
        reply_markup=MENU_KB_TEACHER
    )

# [ADDED] Переключение обратно в режим группы
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

# [ADDED] Переключение в режим преподавателя — восстанавливает последнего если был
async def cmd_switch_to_teacher(message: Message, state: FSMContext, schedules: ScheduleClient):
    fsm_data = await state.get_data()
    teacher_id = fsm_data.get("teacher_prep_id")
    teacher_name = fsm_data.get("teacher_name")
    
    if teacher_id and teacher_name:
        # Восстанавливаем последнего выбранного преподавателя без нового поиска
        await state.set_data({**fsm_data, "mode": "teacher"})
        await state.set_state(None)
        await message.answer(f"Преподаватель: {html.escape(teacher_name)}", reply_markup=MENU_KB_TEACHER)
    else:
        # Первый раз — запускаем поиск
        await cmd_teacher_schedule(message, state)


# ==================== ADMIN HANDLERS ====================

# [ADDED] /broadcast — рассылка всем пользователям
async def cmd_broadcast(message: Message, state: FSMContext):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    
    await state.set_state(BroadcastFlow.waiting_text)
    await message.answer(
        "Введи текст для рассылки (HTML разметка поддерживается):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BTN_CANCEL)]],
            resize_keyboard=True
        )
    )

# [ADDED] Получение текста и выполнение рассылки
async def on_broadcast_text(message: Message, state: FSMContext, bot: Bot, db: Database):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    
    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Рассылка отменена.", reply_markup=MENU_KB_GROUP)
        return
    
    text = message.text or ""
    if not text.strip():
        await message.answer("Текст пустой. Введи снова или нажми Отмена.")
        return
    
    await state.clear()
    await message.answer("Начинаю рассылку...")
    
    rows = await db.fetchall("SELECT user_id FROM registered_users")
    user_ids = [row[0] for row in rows] if rows else []
    
    sent = 0
    failed = 0
    
    for uid in user_ids:
        try:
            await bot.send_message(uid, text)
            sent += 1
        except Exception as e:
            failed += 1
            logging.getLogger(__name__).warning(f"Broadcast failed for {uid}: {e}")
        await asyncio.sleep(0.05)  # не давим на Telegram API
    
    await message.answer(
        f"✅ Рассылка завершена.\n"
        f"Отправлено: {sent}\n"
        f"Ошибок: {failed}\n"
        f"Всего пользователей: {len(user_ids)}",
        reply_markup=MENU_KB_GROUP
    )

# [ADDED] /stats — статистика + CSV
async def cmd_stats(message: Message, bot: Bot, db: Database, store: UserSettingsStore):
    if not message.from_user or message.from_user.id != ADMIN_USER_ID:
        return
    
    # Общее количество пользователей
    total_row = await db.fetchone("SELECT COUNT(*) FROM registered_users")
    total = total_row[0] if total_row else 0
    
    # Краткое сообщение — только итоговая цифра
    await message.answer(f"<b>📊 Статистика бота</b>\nВсего пользователей: <b>{total}</b>")
    
    # CSV: ВСЕ пользователи из registered_users, даже без группы
    all_rows = await db.fetchall(
        "SELECT ru.user_id, us.group_title, us.course "
        "FROM registered_users ru "
        "LEFT JOIN user_settings us ON ru.user_id = us.user_id "
        "ORDER BY ru.user_id"
    )
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "group_name", "course"])
    
    for row in (all_rows or []):
        user_id = row[0]
        group_name = row[1] if row[1] is not None else ""
        course = row[2] if row[2] is not None else ""
        writer.writerow([user_id, group_name, course])
    
    csv_bytes = output.getvalue().encode("utf-8-sig")
    filename = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    await bot.send_document(
        ADMIN_USER_ID,
        BufferedInputFile(csv_bytes, filename=filename)
    )


# ==================== FSM CLEANUP TASK ====================
async def fsm_cleanup_task(fsm_storage: MySQLStorage):
    """Периодическая очистка FSM записей раз в 3 часа."""
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
    
    bot = Bot(
        token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=AiohttpSession(proxy=os.getenv("TELEGRAM_PROXY"))
    )
    
    db = Database(
        os.getenv("DB_HOST", "localhost"),
        int(os.getenv("DB_PORT", "3306")),
        os.getenv("DB_USER", "istu_bot"),
        os.getenv("DB_PASSWORD", ""),
        os.getenv("DB_NAME", "istu_bot")
    )
    await db.connect()
    
    store = UserSettingsStore(db)
    await store.initialize()
    
    # [ADDED] Инициализируем таблицу зарегистрированных пользователей
    await init_registered_users_table(db)
    
    # [ADDED] Импортируем user_id из файла (INSERT IGNORE — безопасно при повторных запусках)
    await import_users_from_file(db, USER_IDS_FILE)
    
    fsm_storage = MySQLStorage(db)
    await fsm_storage.initialize()
    
    dp = Dispatcher(storage=fsm_storage)
    
    async with aiohttp.ClientSession(headers={"User-Agent": "ISTU-Bot/2.2"}) as http:
        schedules = ScheduleClient(http)
        ref_cache = ReferenceDataCache(schedules)
        
        dp["store"] = store
        dp["schedules"] = schedules
        dp["ref_cache"] = ref_cache
        dp["db"] = db  # [ADDED] передаём db для admin handlers
        
        # ---- Оригинальные обработчики (не менять порядок) ----
        dp.message.register(cmd_start, Command("start"))
        dp.message.register(cmd_start, F.text == BTN_CHANGE_GROUP)
        dp.message.register(cmd_report, F.text == BTN_REPORT)
        dp.message.register(on_setup_institute, SetupFlow.institute)
        dp.message.register(on_setup_course, SetupFlow.course)
        dp.message.register(on_setup_group, SetupFlow.group)
        dp.message.register(on_report_message, ReportFlow.report)
        dp.message.register(on_menu, F.text.in_({BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK}))
        
        # ---- [ADDED] Новые обработчики преподавателей ----
        dp.message.register(cmd_switch_to_teacher, F.text == BTN_TEACHER_SCHEDULE)  # восстанавливает или ищет
        dp.message.register(cmd_teacher_schedule, F.text == BTN_CHANGE_TEACHER)  # всегда новый поиск
        dp.message.register(cmd_switch_to_group, F.text == BTN_GROUP_SCHEDULE)
        dp.message.register(on_teacher_search, TeacherFlow.search)
        dp.message.register(on_teacher_select, TeacherFlow.select)
        
        # ---- [ADDED] Админ-команды ----
        dp.message.register(cmd_broadcast, Command("broadcast"))
        dp.message.register(cmd_stats, Command("stats"))
        dp.message.register(on_broadcast_text, BroadcastFlow.waiting_text)
        
        cleanup_task = asyncio.create_task(fsm_cleanup_task(fsm_storage))
        
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            me = await bot.get_me()
            logging.info(f"Bot @{me.username} started")
            
            # [ADDED] Регистрируем команды в Telegram
            # Для обычных пользователей — только /start
            await bot.set_my_commands(
                [BotCommand(command="start", description="Запустить бота")]
            )
            # Для администратора — все команды включая /broadcast и /stats
            try:
                await bot.set_my_commands(
                    [
                        BotCommand(command="start", description="Запустить бота"),
                        BotCommand(command="broadcast", description="📢 Рассылка всем"),
                        BotCommand(command="stats", description="📊 Статистика + CSV"),
                    ],
                    scope=BotCommandScopeChat(chat_id=ADMIN_USER_ID)
                )
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
