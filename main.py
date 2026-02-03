import asyncio
import logging
import json
import html
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramNetworkError
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from bs4 import BeautifulSoup


try:
    IRKUTSK_TZ = ZoneInfo("Asia/Irkutsk")
except ZoneInfoNotFoundError:
    IRKUTSK_TZ = timezone(timedelta(hours=8))
BASE_SCHEDULE_URL = "https://www.istu.edu/schedule/"

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

ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID") or "0")

MENU_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=BTN_TODAY), KeyboardButton(text=BTN_TOMORROW)],
        [KeyboardButton(text=BTN_THIS_WEEK), KeyboardButton(text=BTN_NEXT_WEEK)],
        [KeyboardButton(text=BTN_CHANGE_GROUP), KeyboardButton(text=BTN_REPORT)],
    ],
    resize_keyboard=True,
)


class SetupFlow(StatesGroup):
    institute = State()
    course = State()
    group = State()


class ReportFlow(StatesGroup):
    report = State()


@dataclass(frozen=True)
class Institute:
    subdiv_id: int
    title: str


@dataclass(frozen=True)
class Group:
    group_id: int
    title: str


@dataclass(frozen=True)
class Lesson:
    start: time
    subject: str
    kind: str
    subgroup: str
    room: str
    teacher: str


@dataclass(frozen=True)
class DaySchedule:
    heading: str
    lessons: list[Lesson]


class UserSettingsStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._data = {}
            return
        try:
            self._data = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(self._data, dict):
                self._data = {}
        except Exception:
            self._data = {}

    def _save(self) -> None:
        self._path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, user_id: int) -> dict[str, Any]:
        raw = self._data.get(str(user_id))
        if isinstance(raw, dict):
            return raw
        return {}

    def set(self, user_id: int, settings: dict[str, Any]) -> None:
        self._data[str(user_id)] = settings
        self._save()


class ScheduleClient:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        self._cache: dict[str, tuple[float, str]] = {}
        self._parsed_cache: dict[str, tuple[float, tuple[bool, list[DaySchedule]]]] = {}
        self._parsed_cache_ttl = float(os.getenv("SCHEDULE_PARSED_CACHE_TTL") or "120")
        self._parsed_cache_max = int(os.getenv("SCHEDULE_PARSED_CACHE_MAX") or "256")
        self._institutes_cache: Optional[list[Institute]] = None
        self._groups_cache: dict[int, dict[int, list[Group]]] = {}

    async def _get_text(self, url: str, params: Optional[dict[str, Any]] = None) -> str:
        key = url + "?" + "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
        now = asyncio.get_running_loop().time()
        cached = self._cache.get(key)
        if cached and now - cached[0] < 30:
            return cached[1]
        async with self._session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=25)) as resp:
            resp.raise_for_status()
            text = await resp.text()
        self._cache[key] = (now, text)
        return text

    def _parsed_cache_key(self, group_id: int, target_date: date) -> str:
        return f"{group_id}:{target_date.isoformat()}"

    def _prune_parsed_cache(self) -> None:
        if not self._parsed_cache:
            return
        now = asyncio.get_running_loop().time()
        ttl = max(1.0, self._parsed_cache_ttl)

        expired = [k for k, (ts, _) in self._parsed_cache.items() if now - ts > ttl]
        for k in expired:
            self._parsed_cache.pop(k, None)

        max_items = max(1, self._parsed_cache_max)
        if len(self._parsed_cache) <= max_items:
            return

        items = sorted(self._parsed_cache.items(), key=lambda kv: kv[1][0])
        for k, _ in items[: max(0, len(items) - max_items)]:
            self._parsed_cache.pop(k, None)

    async def list_institutes(self) -> list[Institute]:
        if self._institutes_cache is not None:
            return self._institutes_cache
        html = await self._get_text(BASE_SCHEDULE_URL)
        soup = BeautifulSoup(html, "html.parser")

        content = soup.select_one("div.content")
        if content is None:
            return []

        institutes: dict[int, str] = {}
        for a in content.select('a[href^="?subdiv="]'):
            href = a.get("href") or ""
            m = re.match(r"^\?subdiv=(\d+)$", href)
            if not m:
                continue
            subdiv_id = int(m.group(1))
            title = a.get_text(" ", strip=True)
            if title:
                institutes[subdiv_id] = title

        self._institutes_cache = [Institute(subdiv_id=k, title=v) for k, v in institutes.items()]
        self._institutes_cache.sort(key=lambda x: x.title.lower())
        return self._institutes_cache

    async def list_groups_by_course(self, subdiv_id: int) -> dict[int, list[Group]]:
        cached = self._groups_cache.get(subdiv_id)
        if cached is not None:
            return cached

        html = await self._get_text(BASE_SCHEDULE_URL, params={"subdiv": subdiv_id})
        soup = BeautifulSoup(html, "html.parser")
        kurs_list = soup.select_one("ul.kurs-list")
        if kurs_list is None:
            self._groups_cache[subdiv_id] = {}
            return {}

        by_course: dict[int, list[Group]] = {}

        for li in kurs_list.find_all("li"):
            ul = li.find("ul")
            if ul is None:
                continue
            header = li.get_text(" ", strip=True)
            m = re.match(r"^Курс\s*(\d+)\b", header)
            if not m:
                continue
            course = int(m.group(1))

            groups: list[Group] = []
            for a in ul.select('a[href^="?group="]'):
                href = a.get("href") or ""
                mg = re.match(r"^\?group=(\d+)$", href)
                if not mg:
                    continue
                group_id = int(mg.group(1))
                title = a.get_text(" ", strip=True)
                if title:
                    groups.append(Group(group_id=group_id, title=title))
            groups.sort(key=lambda g: g.title.lower())
            if groups:
                by_course[course] = groups

        self._groups_cache[subdiv_id] = by_course
        return by_course

    async def get_week_schedule(self, group_id: int, target_date: date) -> tuple[bool, list[DaySchedule]]:
        self._prune_parsed_cache()
        cache_key = self._parsed_cache_key(group_id, target_date)
        cached = self._parsed_cache.get(cache_key)
        if cached is not None:
            return cached[1]

        html = await self._get_text(
            BASE_SCHEDULE_URL,
            params={"group": group_id, "date": f"{target_date.year}-{target_date.month}-{target_date.day}"},
        )
        soup = BeautifulSoup(html, "html.parser")

        week_block = soup.select_one("#dateweek")
        week_text = week_block.get_text(" ", strip=True).lower() if week_block else ""
        if "нечет" in week_text or "нечёт" in week_text:
            page_is_odd_week = True
        elif "четн" in week_text or "чётн" in week_text:
            page_is_odd_week = False
        else:
            page_is_odd_week = False

        is_odd_week = not page_is_odd_week

        content = soup.select_one("div.content")
        if content is None:
            return (is_odd_week, [])

        days: list[DaySchedule] = []
        for h in content.select("h3.day-heading"):
            heading = h.get_text(" ", strip=True)
            lines = h.find_next_sibling("div", class_="class-lines")
            if lines is None:
                continue

            lessons: list[Lesson] = []
            for item in lines.select("div.class-line-item"):
                time_el = item.select_one("div.class-time")
                if time_el is None:
                    continue
                start = _parse_time(time_el.get_text(" ", strip=True))
                if start is None:
                    continue

                tails: list[Any] = []
                tails.extend(item.select("div.class-tail.class-all-week"))

                tail_selector = "div.class-tail.class-odd-week" if is_odd_week else "div.class-tail.class-even-week"
                tail_week = item.select_one(tail_selector)
                if tail_week is not None:
                    tails.append(tail_week)

                if not tails:
                    continue

                for tail in tails:
                    if tail.get_text(" ", strip=True).lower() == "свободно":
                        continue

                    for subject_el in tail.select("div.class-pred"):
                        subject = subject_el.get_text(" ", strip=True)
                        if not subject:
                            continue

                        kind_info = subject_el.find_previous_sibling("div", class_="class-info")
                        kind_text = kind_info.get_text(" ", strip=True) if kind_info else ""
                        kind = _extract_lesson_kind(kind_text)

                        teacher = "—"
                        if kind_info is not None:
                            teacher_links = kind_info.select('a[href^="?prep="]')
                            teacher_names = [a.get_text(" ", strip=True) for a in teacher_links]
                            teacher_names = [t for t in teacher_names if t]
                            if teacher_names:
                                teacher = ", ".join(teacher_names)

                        group_info = _find_next_sibling(subject_el, "class-info")
                        subgroup = _extract_subgroup(group_info.get_text(" ", strip=True) if group_info else "")

                        aud_el = _find_next_sibling(subject_el, "class-aud")
                        room = aud_el.get_text(" ", strip=True) if aud_el else "—"

                        lessons.append(
                            Lesson(
                                start=start,
                                subject=subject,
                                kind=kind,
                                subgroup=subgroup,
                                room=room or "—",
                                teacher=teacher or "—",
                            )
                        )

            days.append(DaySchedule(heading=heading, lessons=lessons))

        result = (is_odd_week, days)
        now = asyncio.get_running_loop().time()
        self._parsed_cache[cache_key] = (now, result)
        self._prune_parsed_cache()
        return result


def _parse_time(value: str) -> Optional[time]:
    m = re.match(r"^(\d{1,2}):(\d{2})$", value.strip())
    if not m:
        return None
    h = int(m.group(1))
    mm = int(m.group(2))
    if not (0 <= h <= 23 and 0 <= mm <= 59):
        return None
    return time(hour=h, minute=mm)


def _find_next_sibling(el: Any, class_name: str) -> Optional[Any]:
    cur = el
    while True:
        cur = cur.find_next_sibling()
        if cur is None:
            return None
        classes = cur.get("class") or []
        if class_name in classes:
            return cur


def _extract_subgroup(text: str) -> str:
    m = re.search(r"подгруппа\s*(\d+)", text.lower())
    if not m:
        return ""
    return f"подгруппа {m.group(1)}"


def _extract_lesson_kind(text: str) -> str:
    t = text.lower()
    if "лекц" in t:
        return "лекция"
    if "практ" in t:
        return "практика"
    if "лаб" in t or "лаборатор" in t:
        return "лаба"
    return ""


RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def _extract_day_month(heading: str) -> Optional[tuple[int, int]]:
    h = heading.lower()
    m = re.search(r",\s*(\d{1,2})\s+([а-яё]+)", h)
    if not m:
        return None
    day = int(m.group(1))
    month = RU_MONTHS.get(m.group(2))
    if month is None:
        return None
    return (day, month)


def _extract_date_from_heading(heading: str, reference: date) -> Optional[date]:
    dm = _extract_day_month(heading)
    if dm is None:
        return None
    day, month = dm
    year = reference.year
    if reference.month == 12 and month == 1:
        year += 1
    if reference.month == 1 and month == 12:
        year -= 1
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _format_day_message(heading: str, lessons: list[Lesson]) -> str:
    sep = "------------------"
    out: list[str] = [f"🍌{heading}", sep]
    if not lessons:
        out.append("нет занятий")
        return "\n".join(out)

    lessons = sorted(lessons, key=lambda l: (l.start, l.subject, l.kind, l.subgroup, l.room, l.teacher))

    blocks: dict[time, list[Lesson]] = {}
    order: list[time] = []
    for lesson in lessons:
        key = lesson.start
        if key not in blocks:
            blocks[key] = []
            order.append(key)
        blocks[key].append(lesson)

    for bi, key in enumerate(order):
        start = key
        start_dt = datetime.combine(date(2000, 1, 1), start)
        end_dt = start_dt + timedelta(minutes=90)

        variants = sorted(blocks[key], key=lambda l: (l.subject, l.kind, l.subgroup, l.room, l.teacher))
        for vi, lesson in enumerate(variants):
            if vi > 0:
                out.append("===")
            kind = f" ({lesson.kind})" if lesson.kind else ""
            out.append(f"{start.strftime('%H:%M')} — {end_dt.time().strftime('%H:%M')} {lesson.subject}{kind}")

            details: list[str] = []
            if lesson.subgroup:
                details.append(lesson.subgroup)
            if lesson.room and lesson.room != "—":
                details.append(lesson.room)
            if lesson.teacher and lesson.teacher != "—":
                details.append(lesson.teacher)
            if details:
                out.append(" • " + " | ".join(details))

        if bi < len(order) - 1:
            out.append(sep)

    return "\n".join(out)


def _chunk(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _is_sshg(title: str) -> bool:
    t = title.lower()
    return "сибирская школа геонаук" in t or "siberian school of geosciences" in t


def _institutes_kb(labels: list[str]) -> ReplyKeyboardMarkup:
    keyboard: list[list[KeyboardButton]] = []
    for row in _chunk(labels, 1):
        keyboard.append([KeyboardButton(text=t) for t in row])
    keyboard.append([KeyboardButton(text=BTN_REPORT)])
    keyboard.append([KeyboardButton(text=BTN_CANCEL)])
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


def _paged_kb(
    options: list[str],
    page: int,
    page_size: int,
    row_size: int,
    show_back: bool,
    show_cancel: bool,
) -> ReplyKeyboardMarkup:
    start = page * page_size
    slice_ = options[start : start + page_size]

    keyboard: list[list[KeyboardButton]] = []
    for row in _chunk(slice_, row_size):
        keyboard.append([KeyboardButton(text=t) for t in row])

    controls: list[KeyboardButton] = []
    if page > 0:
        controls.append(KeyboardButton(text=BTN_PAGE_PREV))
    if start + page_size < len(options):
        controls.append(KeyboardButton(text=BTN_PAGE_NEXT))
    if controls:
        keyboard.append(controls)

    keyboard.append([KeyboardButton(text=BTN_REPORT)])

    if show_back:
        keyboard.append([KeyboardButton(text=BTN_BACK)])
    if show_cancel:
        keyboard.append([KeyboardButton(text=BTN_CANCEL)])

    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)


async def _ensure_user_ready(message: Message, store: UserSettingsStore) -> Optional[dict[str, Any]]:
    if message.from_user is None:
        return None
    settings = store.get(message.from_user.id)
    if not settings.get("group_id"):
        await message.answer("Сначала выбери группу: нажми «Изменить группу» или /start", reply_markup=MENU_KB)
        return None
    return settings


async def cmd_start(message: Message, state: FSMContext, schedules: ScheduleClient) -> None:
    await state.clear()
    await state.set_state(SetupFlow.institute)

    institutes = await schedules.list_institutes()
    if not institutes:
        await message.answer("Не получилось загрузить список институтов. Попробуй позже.")
        return

    await state.update_data(
        institutes=[
            {
                "subdiv_id": i.subdiv_id,
                "title": i.title,
                "label": "СШГ" if _is_sshg(i.title) else i.title,
            }
            for i in institutes
        ],
    )
    labels = [("СШГ" if _is_sshg(i.title) else i.title) for i in institutes]
    await message.answer("Выбери институт:", reply_markup=_institutes_kb(labels))


async def cmd_change(message: Message, state: FSMContext, schedules: ScheduleClient) -> None:
    await cmd_start(message, state, schedules)


async def on_setup_institute(message: Message, state: FSMContext, schedules: ScheduleClient) -> None:
    data = await state.get_data()
    institutes_raw = data.get("institutes") or []
    institutes = [
        {"subdiv_id": int(i["subdiv_id"]), "title": str(i["title"]), "label": str(i.get("label") or i.get("title") or "")}
        for i in institutes_raw
        if "subdiv_id" in i and "title" in i
    ]
    if not institutes:
        await cmd_start(message, state, schedules)
        return

    labels = [i["label"] for i in institutes]

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок", reply_markup=MENU_KB)
        return

    selected = next((i for i in institutes if i["label"] == (message.text or "")), None)
    if selected is None:
        await message.answer("Выбери институт кнопкой.", reply_markup=_institutes_kb(labels))
        return

    by_course = await schedules.list_groups_by_course(int(selected["subdiv_id"]))
    courses = sorted(by_course.keys())
    if not courses:
        await message.answer("Не нашёл курсы для этого института. Выбери другой.")
        return

    await state.set_state(SetupFlow.course)
    await state.update_data(
        subdiv_id=int(selected["subdiv_id"]),
        courses=courses,
        by_course={str(k): [{"group_id": g.group_id, "title": g.title} for g in v] for k, v in by_course.items()},
    )
    await message.answer("Выбери курс:", reply_markup=_paged_kb([str(c) for c in courses], page=0, page_size=12, row_size=3, show_back=True, show_cancel=True))


async def on_setup_course(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    courses = data.get("courses") or []
    courses = [int(c) for c in courses]
    if not courses:
        await message.answer("Список курсов пуст. Нажми «Изменить группу» или /start", reply_markup=MENU_KB)
        await state.clear()
        return

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок", reply_markup=MENU_KB)
        return
    if message.text == BTN_BACK:
        await state.set_state(SetupFlow.institute)
        institutes_raw = data.get("institutes") or []
        labels = [str(i.get("label") or i.get("title") or "") for i in institutes_raw if "subdiv_id" in i and "title" in i]
        await message.answer("Выбери институт:", reply_markup=_institutes_kb(labels))
        return

    try:
        course = int((message.text or "").strip())
    except ValueError:
        await message.answer("Выбери курс кнопкой.", reply_markup=_paged_kb([str(c) for c in courses], page=0, page_size=12, row_size=3, show_back=True, show_cancel=True))
        return

    if course not in courses:
        await message.answer("Выбери курс кнопкой.", reply_markup=_paged_kb([str(c) for c in courses], page=0, page_size=12, row_size=3, show_back=True, show_cancel=True))
        return

    by_course = data.get("by_course") or {}
    groups_raw = by_course.get(str(course)) or []
    groups = [Group(group_id=int(g["group_id"]), title=str(g["title"])) for g in groups_raw if "group_id" in g and "title" in g]
    groups.sort(key=lambda g: g.title.lower())
    if not groups:
        await message.answer("Не нашёл группы на этом курсе. Выбери другой курс.")
        return

    await state.set_state(SetupFlow.group)
    await state.update_data(course=course, group_page=0, groups=[{"group_id": g.group_id, "title": g.title} for g in groups])
    await message.answer("Выбери группу:", reply_markup=_paged_kb([g.title for g in groups], page=0, page_size=10, row_size=2, show_back=True, show_cancel=True))


async def on_setup_group(message: Message, state: FSMContext, store: UserSettingsStore) -> None:
    data = await state.get_data()
    groups_raw = data.get("groups") or []
    groups = [Group(group_id=int(g["group_id"]), title=str(g["title"])) for g in groups_raw if "group_id" in g and "title" in g]
    groups.sort(key=lambda g: g.title.lower())
    if not groups:
        await message.answer("Список групп пуст. Нажми «Изменить группу» или /start", reply_markup=MENU_KB)
        await state.clear()
        return

    page = int(data.get("group_page") or 0)
    titles = [g.title for g in groups]

    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок", reply_markup=MENU_KB)
        return
    if message.text == BTN_BACK:
        await state.set_state(SetupFlow.course)
        courses = data.get("courses") or []
        await message.answer("Выбери курс:", reply_markup=_paged_kb([str(c) for c in courses], page=0, page_size=12, row_size=3, show_back=True, show_cancel=True))
        return
    if message.text == BTN_PAGE_PREV:
        page = max(0, page - 1)
        await state.update_data(group_page=page)
        await message.answer("Выбери группу:", reply_markup=_paged_kb(titles, page=page, page_size=10, row_size=2, show_back=True, show_cancel=True))
        return
    if message.text == BTN_PAGE_NEXT:
        page = min(max(0, (len(titles) - 1) // 10), page + 1)
        await state.update_data(group_page=page)
        await message.answer("Выбери группу:", reply_markup=_paged_kb(titles, page=page, page_size=10, row_size=2, show_back=True, show_cancel=True))
        return

    selected = next((g for g in groups if g.title == (message.text or "")), None)
    if selected is None:
        await message.answer("Выбери группу кнопкой.", reply_markup=_paged_kb(titles, page=page, page_size=10, row_size=2, show_back=True, show_cancel=True))
        return

    if message.from_user is None:
        return

    store.set(
        message.from_user.id,
        {
            "group_id": selected.group_id,
            "group_title": selected.title,
            "subdiv_id": data.get("subdiv_id"),
            "course": data.get("course"),
        },
    )
    await state.clear()
    await message.answer(f"Ок, группа: {selected.title}", reply_markup=MENU_KB)


async def cmd_report(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ReportFlow.report)
    await state.update_data(report_text="", report_photo=None)
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=BTN_CANCEL)]], resize_keyboard=True)
    await message.answer("Подробно опиши проблему. Можно прикрепить фото.", reply_markup=kb)


async def on_report_message(message: Message, state: FSMContext, store: UserSettingsStore, bot: Bot) -> None:
    if message.text == BTN_CANCEL:
        await state.clear()
        await message.answer("Ок", reply_markup=MENU_KB)
        return

    data = await state.get_data()
    report_text = str(data.get("report_text") or "")
    report_photo = data.get("report_photo")

    incoming_text = ""
    if message.caption:
        incoming_text = message.caption.strip()
    elif message.text:
        incoming_text = message.text.strip()

    if message.photo:
        if report_photo is not None:
            await message.answer("Можно прикрепить только одно фото.")
            return
        report_photo = message.photo[-1].file_id

    if incoming_text:
        if incoming_text in {BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK, BTN_CHANGE_GROUP, BTN_REPORT}:
            await message.answer("Подробно опиши проблему текстом. Можно прикрепить фото.")
            return
        report_text = incoming_text

    await state.update_data(report_text=report_text, report_photo=report_photo)

    if not report_text and report_photo is not None and message.photo and not message.caption:
        await message.answer("Фото получил. Теперь опиши проблему текстом.")
        return

    if not report_text and report_photo is None:
        await message.answer("Пришли текстовое описание и, если нужно, фото.")
        return

    user = message.from_user
    user_line = "неизвестно"
    if user is not None:
        uname = f"@{user.username}" if user.username else "—"
        user_line = f"{user.full_name}/{uname}".strip()

    settings = store.get(user.id) if user is not None else {}
    group_title = settings.get("group_title") or ""
    group_id = settings.get("group_id") or ""

    safe_user = html.escape(user_line)
    safe_group = html.escape(str(group_title or group_id).strip())
    now_str = datetime.now(IRKUTSK_TZ).strftime("%Y-%m-%d   %H:%M:%S")

    body = report_text.strip()
    if len(body) > 3000:
        body = body[:3000] + "…"
    safe_body = html.escape(body)

    parts: list[str] = ["<i>Баг-репорт</i>", f"<b>Пользователь:</b> {safe_user}"]
    if safe_group:
        parts.append(f"<b>Группа:</b> {safe_group}")
    parts.append(f"<b>Время:</b> {html.escape(now_str)}")
    if safe_body:
        parts.append("")
        parts.append(safe_body)
    admin_text = "\n".join(parts)

    if ADMIN_USER_ID > 0:
        try:
            if report_photo is not None:
                await bot.send_photo(chat_id=ADMIN_USER_ID, photo=report_photo, caption=admin_text[:1024])
                if len(admin_text) > 1024:
                    await bot.send_message(chat_id=ADMIN_USER_ID, text=admin_text)
            else:
                await bot.send_message(chat_id=ADMIN_USER_ID, text=admin_text)
        except Exception:
            logging.exception("Failed to send bug report to admin")

    await state.clear()
    await message.answer("Спасибо! Скоро всё исправим.", reply_markup=MENU_KB)


async def on_menu(message: Message, schedules: ScheduleClient, store: UserSettingsStore) -> None:
    settings = await _ensure_user_ready(message, store)
    if settings is None:
        return

    group_id = int(settings["group_id"])
    now = datetime.now(IRKUTSK_TZ)

    if message.text == BTN_TODAY:
        await _send_day(message, schedules, group_id, now.date(), "на сегодня")
        return
    if message.text == BTN_TOMORROW:
        await _send_day(message, schedules, group_id, now.date() + timedelta(days=1), "на завтра")
        return
    if message.text == BTN_THIS_WEEK:
        monday = now.date() - timedelta(days=now.date().weekday())
        await _send_week(message, schedules, group_id, monday, "текущая неделя")
        return
    if message.text == BTN_NEXT_WEEK:
        monday = now.date() - timedelta(days=now.date().weekday()) + timedelta(days=7)
        await _send_week(message, schedules, group_id, monday, "следующая неделя")
        return


async def _send_day(message: Message, schedules: ScheduleClient, group_id: int, target_date: date, title: str) -> None:
    try:
        _, days = await schedules.get_week_schedule(group_id, target_date)
    except Exception:
        await message.answer("Не получилось загрузить расписание. Попробуй позже.")
        return

    target = None
    for day in days:
        day_date = _extract_date_from_heading(day.heading, target_date)
        if day_date == target_date:
            target = day
            break

    if target is None:
        await message.answer(f"Не нашёл расписание {title}.")
        return

    await message.answer(_format_day_message(target.heading, target.lessons))


async def _send_week(message: Message, schedules: ScheduleClient, group_id: int, monday: date, title: str) -> None:
    try:
        _, days = await schedules.get_week_schedule(group_id, monday)
    except Exception:
        await message.answer("Не получилось загрузить расписание. Попробуй позже.")
        return

    await message.answer(title)

    if not days:
        await message.answer("Не нашёл расписание на неделю.")
        return

    week_end = monday + timedelta(days=6)
    picked: list[tuple[date, DaySchedule]] = []
    for day in days:
        day_date = _extract_date_from_heading(day.heading, monday)
        if day_date is None:
            continue
        if monday <= day_date <= week_end:
            picked.append((day_date, day))
    picked.sort(key=lambda x: x[0])

    if not picked:
        await message.answer("Не нашёл расписание на неделю.")
        return

    for _, day in picked:
        await message.answer(_format_day_message(day.heading, day.lessons))


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN env var is required")

    tg_proxy = os.getenv("TELEGRAM_PROXY")
    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=AiohttpSession(proxy=tg_proxy) if tg_proxy else AiohttpSession(),
    )
    dp = Dispatcher(storage=MemoryStorage())

    store = UserSettingsStore(Path(__file__).with_name("user_settings.json"))

    http_session = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"})
    try:
        schedules = ScheduleClient(session=http_session)
        dp["store"] = store
        dp["schedules"] = schedules

        dp.message.register(cmd_start, Command("start"))
        dp.message.register(cmd_change, Command("change"))
        dp.message.register(cmd_change, F.text == BTN_CHANGE_GROUP)
        dp.message.register(cmd_report, F.text == BTN_REPORT)

        dp.message.register(on_setup_institute, SetupFlow.institute)
        dp.message.register(on_setup_course, SetupFlow.course)
        dp.message.register(on_setup_group, SetupFlow.group)
        dp.message.register(on_report_message, ReportFlow.report)

        dp.message.register(on_menu, F.text.in_({BTN_TODAY, BTN_TOMORROW, BTN_THIS_WEEK, BTN_NEXT_WEEK}))

        delay = 5
        while True:
            try:
                await bot.delete_webhook(drop_pending_updates=True)
                me = await bot.get_me()
                logging.info("Bot started: @%s (%s)", me.username, me.id)
                await dp.start_polling(bot)
                return
            except TelegramNetworkError as e:
                logging.warning("Telegram недоступен (%s). Повтор через %sс", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
    except Exception:
        logging.exception("Bot stopped with error")
        raise
    finally:
        await http_session.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
