import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
import aiosqlite

BASE = "https://www.swapi.tech/api"
DB_PATH = "swapi.db"

CONCURRENCY = 20        # одновременных запросов деталей
PAGE_LIMIT = 100        # элементов на страницу

async def fetch_json(session: aiohttp.ClientSession, url: str, *, retries: int = 3) -> Optional[Dict[str, Any]]:
    """GET JSON с простыми ретраями и таймаутом."""
    delay = 0.5
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    # другие коды — не ретраим
                    txt = await resp.text()
                    print(f"HTTP {resp.status} for {url}: {txt[:200]}")
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt == retries - 1:
                print(f"Request failed for {url}: {e}")
                return None
            await asyncio.sleep(delay)
            delay *= 2
    return None

async def iter_people_uids(session: aiohttp.ClientSession) -> List[str]:
    """Собираем все uid персонажей, идя по страницам, пока не закончатся результаты."""
    uids: List[str] = []
    page = 1
    while True:
        url = f"{BASE}/people?page={page}&limit={PAGE_LIMIT}"
        data = await fetch_json(session, url)
        if not data or not data.get("results"):
            break
        for item in data["results"]:
            uid = str(item.get("uid") or "").strip()
            if uid:
                uids.append(uid)
                
        page += 1
    return uids

async def upsert_person(db: aiosqlite.Connection, person: Dict[str, Any]) -> None:
    """Вставка/обновление записи о персонаже."""
    props = person["result"]["properties"]
    uid = int(person["result"]["uid"])
    values = [
        uid,
        props.get("name") or None,
        props.get("birth_year") or None,
        props.get("eye_color") or None,
        props.get("gender") or None,
        props.get("hair_color") or None,
        props.get("homeworld") or None,
        props.get("mass") or None,
        props.get("skin_color") or None,
    ]
    sql = """
    INSERT INTO people (id, name, birth_year, eye_color, gender, hair_color, homeworld, mass, skin_color)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
        name=excluded.name,
        birth_year=excluded.birth_year,
        eye_color=excluded.eye_color,
        gender=excluded.gender,
        hair_color=excluded.hair_color,
        homeworld=excluded.homeworld,
        mass=excluded.mass,
        skin_color=excluded.skin_color
    """
    await db.execute(sql, values)

async def fetch_and_store_one(uid: str, session: aiohttp.ClientSession, db: aiosqlite.Connection, sem: asyncio.Semaphore) -> None:
    """Грузим детали по одному персонажу и пишем в БД."""
    url = f"{BASE}/people/{uid}"
    async with sem:
        data = await fetch_json(session, url)
    if data and data.get("result") and data["result"].get("properties"):
        await upsert_person(db, data)
    else:
        print(f"⚠️ skipped uid={uid}")

async def main() -> None:
    # На всякий случай создадим таблицу, если миграцию не запускали
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY,
            name TEXT,
            birth_year TEXT,
            eye_color TEXT,
            gender TEXT,
            hair_color TEXT,
            homeworld TEXT,
            mass TEXT,
            skin_color TEXT
        )
        """)
        await db.commit()

        async with aiohttp.ClientSession(headers={"Accept": "application/json"}) as session:
            print("➡️  Collecting uids...")
            uids = await iter_people_uids(session)
            print(f"Found {len(uids)} uids")

            sem = asyncio.Semaphore(CONCURRENCY)
            CHUNK = 200
            for i in range(0, len(uids), CHUNK):
                batch = uids[i:i+CHUNK]
                await asyncio.gather(*[
                    fetch_and_store_one(uid, session, db, sem) for uid in batch
                ])
                await db.commit()
                print(f"✅ Saved {i + len(batch)} / {len(uids)}")

            print("🎉 Done")

if __name__ == "__main__":
    asyncio.run(main())
    