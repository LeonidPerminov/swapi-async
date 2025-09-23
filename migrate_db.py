import sqlite3

DB_PATH = "swapi.db"

DDL = """
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
);
"""

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()
        print("✅ Migration complete: table 'people' is ready in swapi.db")
    finally:
        conn.close()
        