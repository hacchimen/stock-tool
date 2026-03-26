import os
import sqlite3

DB_PATH = os.path.join("data", "jquants_prices.db")


def normalize_search_code(code: str) -> str:
    """
    J-Quantsのコードを検索用に変換する。
    例:
      135A0 -> 135A
      72030 -> 7203
      54010 -> 5401
      130A0 -> 130A
    末尾が 0 なら落とす。
    """
    s = str(code).strip().upper()
    if s.endswith("0"):
        return s[:-1]
    return s


def main():
    if not os.path.exists(DB_PATH):
        print("DBファイルがありません。")
        return

    conn = sqlite3.connect(DB_PATH)

    try:
        rows = conn.execute("SELECT code FROM master").fetchall()

        for (code,) in rows:
            search_code = normalize_search_code(code)
            conn.execute(
                "UPDATE master SET search_code = ? WHERE code = ?",
                (search_code, code),
            )

        conn.commit()
        print(f"完了: {len(rows)} 件更新しました。")

    finally:
        conn.close()


if __name__ == "__main__":
    main()