import os
import sqlite3
import pandas as pd

DB_PATH = os.path.join("data", "jquants_prices.db")


def normalize_code(value: str) -> str:
    return str(value).strip().upper()


def main():
    code = input("確認したい銘柄コードを入力してください: ").strip()
    search_code = normalize_code(code)

    if not os.path.exists(DB_PATH):
        print("DBファイルがありません。")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        print("\n=== master テーブル確認 ===")
        df_master = pd.read_sql_query(
            """
            SELECT code, search_code, company_name
            FROM master
            WHERE UPPER(search_code) = ?
               OR UPPER(code) = ?
               OR UPPER(code) LIKE ?
            ORDER BY code
            """,
            conn,
            params=[search_code, search_code, search_code + "%"],
        )
        print(df_master)

        print("\n=== prices テーブル確認（code 完全一致）===")
        df_prices_exact = pd.read_sql_query(
            """
            SELECT code, MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS cnt
            FROM prices
            WHERE UPPER(code) = ?
            GROUP BY code
            """,
            conn,
            params=[search_code],
        )
        print(df_prices_exact)

        print("\n=== prices テーブル確認（前方一致）===")
        df_prices_like = pd.read_sql_query(
            """
            SELECT code, MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS cnt
            FROM prices
            WHERE UPPER(code) LIKE ?
            GROUP BY code
            ORDER BY code
            """,
            conn,
            params=[search_code + "%"],
        )
        print(df_prices_like)

    finally:
        conn.close()


if __name__ == "__main__":
    main()