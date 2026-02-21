# database.py — MySQL Database Layer
import logging
import json
from typing import Optional, Any, Mapping
from contextlib import asynccontextmanager

import aiomysql
from aiogram.fsm.state import State
from aiogram.fsm.storage.base import BaseStorage, StorageKey

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._pool: Optional[aiomysql.Pool] = None
    
    async def connect(self):
        if self._pool:
            return
        self._pool = await aiomysql.create_pool(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            db=self._database,
            autocommit=True,
            minsize=2,
            maxsize=10,
            charset='utf8mb4'
        )
        logger.info(f"DB connected: {self._host}:{self._port}/{self._database}")
    
    async def disconnect(self):
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            logger.info("DB disconnected")
    
    @asynccontextmanager
    async def cursor(self):
        if not self._pool:
            raise RuntimeError("Database not connected")
        conn = await self._pool.acquire()
        cur = await conn.cursor()
        try:
            yield cur
        finally:
            await cur.close()
            self._pool.release(conn)
    
    async def execute(self, query: str, params: tuple = None):
        async with self.cursor() as cur:
            await cur.execute(query, params)
    
    async def fetchone(self, query: str, params: tuple = None):
        async with self.cursor() as cur:
            await cur.execute(query, params)
            return await cur.fetchone()
    
    async def fetchall(self, query: str, params: tuple = None):
        async with self.cursor() as cur:
            await cur.execute(query, params)
            return await cur.fetchall()


class UserSettingsStore:
    def __init__(self, db: Database):
        self._db = db
    
    async def initialize(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id BIGINT PRIMARY KEY,
                group_id INT NOT NULL,
                group_title VARCHAR(100) NOT NULL,
                subdiv_id INT,
                course INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_group (group_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        logger.info("user_settings table initialized")
    
    async def set(self, user_id: int, settings: dict):
        await self._db.execute("""
            INSERT INTO user_settings (user_id, group_id, group_title, subdiv_id, course)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                group_id = VALUES(group_id),
                group_title = VALUES(group_title),
                subdiv_id = VALUES(subdiv_id),
                course = VALUES(course),
                updated_at = CURRENT_TIMESTAMP
        """, (
            user_id,
            settings.get("group_id"),
            settings.get("group_title"),
            settings.get("subdiv_id"),
            settings.get("course")
        ))
    
    async def get(self, user_id: int) -> dict:
        row = await self._db.fetchone(
            "SELECT group_id, group_title, subdiv_id, course FROM user_settings WHERE user_id = %s",
            (user_id,)
        )
        if row:
            return {
                "group_id": row[0],
                "group_title": row[1],
                "subdiv_id": row[2],
                "course": row[3]
            }
        return {}
    
    async def count(self) -> int:
        row = await self._db.fetchone("SELECT COUNT(*) FROM user_settings")
        return row[0] if row else 0


class MySQLStorage(BaseStorage):
    def __init__(self, db: Database):
        self._db = db

    async def initialize(self):
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS fsm_context (
                fsm_key VARCHAR(512) PRIMARY KEY,
                state VARCHAR(255) NULL,
                data_json LONGTEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated_at (updated_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        logger.info("FSM table initialized")

    def _build_key(self, key: StorageKey) -> str:
        parts = ["fsm", str(key.bot_id)]
        business_connection_id = getattr(key, "business_connection_id", None)
        if business_connection_id:
            parts.append(str(business_connection_id))
        parts.append(str(key.chat_id))
        thread_id = getattr(key, "thread_id", None)
        if thread_id:
            parts.append(str(thread_id))
        parts.append(str(key.user_id))
        destiny = getattr(key, "destiny", "default")
        parts.append(str(destiny))
        return ":".join(parts)

    @staticmethod
    def _state_to_str(state: Any) -> Optional[str]:
        if state is None:
            return None
        if isinstance(state, State):
            return state.state
        return str(state)

    async def set_state(self, key: StorageKey, state: Any = None) -> None:
        fsm_key = self._build_key(key)
        state_str = self._state_to_str(state)
        await self._db.execute(
            """
            INSERT INTO fsm_context (fsm_key, state, data_json)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE state = VALUES(state)
            """,
            (fsm_key, state_str, "{}"),
        )

    async def get_state(self, key: StorageKey) -> Optional[str]:
        fsm_key = self._build_key(key)
        row = await self._db.fetchone("SELECT state FROM fsm_context WHERE fsm_key = %s", (fsm_key,))
        return row[0] if row else None

    async def set_data(self, key: StorageKey, data: Mapping[str, Any]) -> None:
        fsm_key = self._build_key(key)
        payload = json.dumps(dict(data), ensure_ascii=False)
        await self._db.execute(
            """
            INSERT INTO fsm_context (fsm_key, state, data_json)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE data_json = VALUES(data_json)
            """,
            (fsm_key, None, payload),
        )

    async def get_data(self, key: StorageKey) -> dict[str, Any]:
        fsm_key = self._build_key(key)
        row = await self._db.fetchone("SELECT data_json FROM fsm_context WHERE fsm_key = %s", (fsm_key,))
        if not row or not row[0]:
            return {}
        try:
            value = json.loads(row[0])
        except json.JSONDecodeError:
            return {}
        return value if isinstance(value, dict) else {}

    async def close(self) -> None:
        return None
