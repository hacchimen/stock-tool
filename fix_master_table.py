import os
import sqlite3

DB_PATH = os.path.join("data", "jquants_prices.db")


def column_exists(conn, table_name, column_name):
    cur = conn.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]
    return column_name in cols


def main():
    if not os.path.exists(DB_PATH):
        print("DBファイルがありません。")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        # master テーブル存在確認
        cur = conn.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name='master'
        """)
        if cur.fetchone() is None:
            print("master テーブルがありません。")
            return

        # search_code 列がなければ追加
        if not column_exists(conn, "master", "search_code"):
            conn.execute("ALTER TABLE master ADD COLUMN search_code TEXT")
            print("search_code 列を追加しました。")
        else:
            print("search_code 列は既にあります。")

        # search_code を code で埋める
        conn.execute("""
            UPDATE master
            SET search_code = UPPER(TRIM(code))
            WHERE search_code IS NULL OR TRIM(search_code) = ''
        """)
        print("search_code を更新しました。")

        # インデックス作成
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_master_search_code
            ON master (search_code)
        """)
        print("インデックスを作成しました。")

        conn.commit()
        print("完了しました。")

    finally:
        conn.close()


if __name__ == "__main__":
    main()