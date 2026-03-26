import os
import sqlite3
import pandas as pd
import jquantsapi

API_KEY = "bo_gGUaExz0kiGNnhD3LX3INwn36Lce2jOcISJaWp1c"
DB_PATH = os.path.join("data", "jquants_prices.db")


def normalize_code(x):
    return str(x).strip().upper()


def first_existing_col(df: pd.DataFrame, candidates: list[str]):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def main():
    if not os.path.exists(DB_PATH):
        print("DBファイルがありません。")
        return

    conn = sqlite3.connect(DB_PATH)
    cli = jquantsapi.ClientV2(api_key=API_KEY)

    print("マスタ取得中...")

    df = None

    # あなたの環境ではこれが本命
    try:
        df = cli.get_eq_master()
    except Exception:
        df = None

    # 念のための保険
    if df is None or len(df) == 0:
        try:
            df = cli.get_listed_info()
        except Exception:
            df = None

    if df is None or len(df) == 0:
        print("マスタ取得失敗")
        conn.close()
        return

    code_col = first_existing_col(df, ["Code", "code", "LocalCode", "local_code"])
    name_col = first_existing_col(
        df,
        ["CompanyName", "company_name", "Name", "name", "IssueName", "IssueNameJa"],
    )

    if code_col is None:
        print("code列が見つかりません。")
        print("列一覧:", df.columns.tolist())
        conn.close()
        return

    rows = []
    for _, r in df.iterrows():
        code = normalize_code(r[code_col])
        name = ""
        if name_col and pd.notna(r.get(name_col)):
            name = str(r.get(name_col)).strip()

        rows.append((code, code, name))

    with conn:
        conn.execute("DELETE FROM master")

        conn.executemany(
            """
            INSERT INTO master (code, search_code, company_name, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            """,
            rows,
        )

    print(f"完了: {len(rows)} 件")

    conn.close()


if __name__ == "__main__":
    main()