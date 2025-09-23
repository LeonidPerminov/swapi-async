import os
import sqlite3

DB_PATH = "swapi.db"

print("База существует:", os.path.exists(DB_PATH))

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Проверим, есть ли таблица people
tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='people'").fetchall()
print("Таблицы:", tables)

if tables:
    # Считаем записи
    count = cur.execute("SELECT COUNT(*) FROM people").fetchone()[0]
    print("Всего записей в people:", count)

    # Выведем имена колонок
    columns = [description[0] for description in cur.execute("PRAGMA table_info(people)")]
    print("Колонки:", columns)

    # Выведем первые 5 строк
    rows = cur.execute("SELECT * FROM people LIMIT 5").fetchall()
    print("Первые 5 строк:")
    for row in rows:
        print(row)

con.close()

