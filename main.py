#!/usr/bin/env python3
import asyncio
import logging
import json
import html
import os
import re
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, List, Tuple
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
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from bs4 import BeautifulSoup

from database import Database, UserSettingsStore, MySQLStorage

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# ==================== CONFIG ====================
try:
    IRKUTSK_TZ = ZoneInfo("Asia/Irkutsk")
except ZoneInfoNotFoundError:
    IRKUTSK_TZ = timezone(timedelta(hours=8))

BASE_SCHEDULE_URL = "https://www.istu.edu/schedule/"
try:
    ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
except ValueError:
    ADMIN_USER_ID = 0

# Network Settings
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
REQUEST_TIMEOUT = 15.0

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

ALL_BTNS = {BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK,
            BTN_CHANGE_GROUP, BTN_REPORT, BTN_BACK, BTN_PAGE_PREV, BTN_PAGE_NEXT, BTN_CANCEL}

MENU_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_GROUP), KeyboardButton(text=BTN_REPORT)],
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
    __slots__ = ('start', 'subject', 'kind', 'subgroup', 'room', 'teacher')
    def __init__(self, start: time, subject: str, kind: str, subgroup: str, room: str, teacher: str):
        self.start = start
        self.subject = subject
        self.kind = kind
        self.subgroup = subgroup
        self.room = room
        self.teacher = teacher

class DaySchedule:
    __slots__ = ('heading', 'lessons')
    def __init__(self, heading: str, lessons: List[Lesson]):
        self.heading = heading
        self.lessons = lessons

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
        await message.answer("Ок", reply_markup=MENU_KB)
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
        kurs_list = soup.select_one("ul.kurs-list")
        if not kurs_list:
            raise RuntimeError("Invalid HTML structure")
        
        by_course: dict[int, List[Group]] = {}
        for li in kurs_list.find_all("li"):
            ul = li.find("ul")
            if not ul:
                continue
            m = RE_COURSE_ID.match(li.get_text(" ", strip=True))
            if not m:
                continue
            course = int(m.group(1))
            
            groups = []
            for a in ul.select('a[href^="?group="]'):
                mg = RE_GROUP_ID.match(a.get("href") or "")
                if mg:
                    title = a.get_text(" ", strip=True)
                    groups.append(Group(int(mg.group(1)), title))
            
            groups.sort(key=lambda g: g.title.lower())
            by_course[course] = groups
        
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

                        lessons.append(Lesson(start, subject, kind, subgroup, room or "—", teacher or "—"))
            
            days.append(DaySchedule(heading, lessons))
        
        result = (odd, days)
        self._cache[key] = (asyncio.get_running_loop().time(), result)
        return result

# ==================== HANDLERS ====================
async def safe_request(message: Message, coro):
    try:
        return await coro
    except Exception as e:
        logging.exception("Schedule request failed")
        await message.answer("⚠️ Не удалось связаться с сервером ИРНИТУ. Попробуй позже.")
        return None

async def cmd_start(message: Message, state: FSMContext, schedules: ScheduleClient):
    await state.clear()
    await state.set_state(SetupFlow.institute)
    
    institutes = await safe_request(message, schedules.list_institutes())
    if not institutes:
        return
    
    data = [
        {"id": i.subdiv_id, "title": i.title, "label": "СШГ" if _is_sshg(i.title) else i.title}
        for i in institutes
    ]
    await state.update_data(institutes=data)
    
    labels = [d["label"] for d in data]
    await state.update_data(inst_page=0)
    await message.answer("Выбери институт:", reply_markup=build_paged_kb(labels, 0, 12, 1, False))

async def on_setup_institute(message: Message, state: FSMContext, schedules: ScheduleClient):
    data = await state.get_data()
    institutes = data.get("institutes", [])
    labels = [i["label"] for i in institutes]
    
    if await handle_navigation(message, state, labels, "inst_page", 12, 1):
        return
    
    selected = next((i for i in institutes if i["label"] == message.text), None)
    if not selected:
        await message.answer("Выбери институт кнопкой.", reply_markup=build_paged_kb(labels, 0, 12, 1, False))
        return
    
    by_course = await safe_request(message, schedules.list_groups_by_course(selected["id"]))
    if not by_course:
        return
    
    courses = sorted(by_course.keys())
    if not courses:
        await message.answer("Нет курсов для этого института.")
        return
    
    await state.set_state(SetupFlow.course)
    await state.update_data(
        subdiv_id=selected["id"],
        courses=courses,
        by_course={k: [{"id": g.group_id, "title": g.title} for g in v] for k, v in by_course.items()},
        course_page=0
    )
    await message.answer("Выбери курс:", reply_markup=build_paged_kb([str(c) for c in courses], 0, 12, 3, True))

async def on_setup_course(message: Message, state: FSMContext):
    data = await state.get_data()
    courses = data.get("courses", [])
    course_labels = [str(c) for c in courses]
    
    back_labels = [i["label"] for i in data.get("institutes", [])]
    if await handle_navigation(message, state, course_labels, "course_page", 12, 3, SetupFlow.institute, back_labels):
        return
    
    try:
        course = int(message.text)
    except ValueError:
        await message.answer("Выбери курс кнопкой.", reply_markup=build_paged_kb(course_labels, 0, 12, 3, True))
        return
    
    if course not in courses:
        await message.answer("Выбери курс кнопкой.", reply_markup=build_paged_kb(course_labels, 0, 12, 3, True))
        return
    
    groups_raw = data.get("by_course", {}).get(str(course), [])
    groups = [Group(g["id"], g["title"]) for g in groups_raw]
    
    if not groups:
        await message.answer("Нет групп на этом курсе.")
        return
    
    await state.set_state(SetupFlow.group)
    await state.update_data(
        course=course,
        groups=[{"id": g.group_id, "title": g.title} for g in groups],
        group_page=0
    )
    await message.answer("Выбери группу:", reply_markup=build_paged_kb([g.title for g in groups], 0, 10, 2, True))

async def on_setup_group(message: Message, state: FSMContext, store: UserSettingsStore):
    data = await state.get_data()
    groups_raw = data.get("groups", [])
    titles = [g["title"] for g in groups_raw]
    
    back_courses = [str(c) for c in data.get("courses", [])]
    if await handle_navigation(message, state, titles, "group_page", 10, 2, SetupFlow.course, back_courses):
        return
    
    selected = next((g for g in groups_raw if g["title"] == message.text), None)
    if not selected:
        page = data.get("group_page", 0)
        await message.answer("Выбери группу кнопкой.", reply_markup=build_paged_kb(titles, page, 10, 2, True))
        return
    
    await store.set(message.from_user.id, {
        "group_id": selected["id"],
        "group_title": selected["title"],
        "subdiv_id": data.get("subdiv_id"),
        "course": data.get("course"),
    })
    await state.clear()
    await message.answer(f"Ок, группа: {selected['title']}", reply_markup=MENU_KB)

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
        await message.answer("Ок", reply_markup=MENU_KB)
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
        
        sent = False
        if ADMIN_USER_ID > 0:
            try:
                if photo:
                    await bot.send_photo(ADMIN_USER_ID, photo, caption=msg[:1024])
                else:
                    await bot.send_message(ADMIN_USER_ID, msg)
                sent = True
            except Exception:
                logging.exception("Send report error")
        
        await state.clear()
        await message.answer("Спасибо! Передал." if sent else "Спасибо! Принял.", reply_markup=MENU_KB)
    else:
        await message.answer("Фото принято. Теперь опиши проблему текстом.")

async def on_menu(message: Message, schedules: ScheduleClient, store: UserSettingsStore):
    settings = await store.get(message.from_user.id) if message.from_user else {}
    gid = settings.get("group_id")
    
    if not gid:
        await message.answer("Сначала выбери группу /start", reply_markup=MENU_KB)
        return
    
    now = datetime.now(IRKUTSK_TZ)
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
    
    # Database
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
    
    fsm_storage = MySQLStorage(db)
    await fsm_storage.initialize()
    
    dp = Dispatcher(storage=fsm_storage)
    
    async with aiohttp.ClientSession(headers={"User-Agent": "ISTU-Bot/2.1"}) as http:
        schedules = ScheduleClient(http)
        dp["store"], dp["schedules"] = store, schedules
        
        dp.message.register(cmd_start, Command("start"))
        dp.message.register(cmd_start, F.text == BTN_CHANGE_GROUP)
        dp.message.register(cmd_report, F.text == BTN_REPORT)
        dp.message.register(on_setup_institute, SetupFlow.institute)
        dp.message.register(on_setup_course, SetupFlow.course)
        dp.message.register(on_setup_group, SetupFlow.group)
        dp.message.register(on_report_message, ReportFlow.report)
        dp.message.register(on_menu, F.text.in_({BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK}))
        
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            me = await bot.get_me()
            logging.info(f"Bot @{me.username} started")
            await dp.start_polling(bot)
        finally:
            await db.disconnect()
            await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
