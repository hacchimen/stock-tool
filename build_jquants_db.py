import os
import sys
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from dateutil import tz
from dateutil.relativedelta import relativedelta
import jquantsapi


# =========================================================
# 設定
# =========================================================
API_KEY = os.getenv("JQUANTS_API_KEY")

if not API_KEY:
    raise ValueError("JQUANTS_API_KEY が設定されていません")

DB_PATH = os.path.join("data", "jquants_prices.db")
YEARS_BACK = 5

# 429対策
CHUNK_DAYS = 7
CHUNK_SLEEP_SECONDS = 12
MAX_RETRIES = 10
RETRY_SLEEP_SECONDS = 30

JST = tz.gettz("Asia/Tokyo")


# =========================================================
# 共通
# =========================================================
def normalize_code(value: object) -> str:
    return str(value).strip().upper()


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")


def first_existing_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def to_float(value):
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def call_with_retry(func, *args, **kwargs):
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            wait = RETRY_SLEEP_SECONDS * attempt
            print(f"[WARN] APIエラー {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                print(f"[INFO] {wait}秒待って再試行します。")
                time.sleep(wait)

    raise last_error


# =========================================================
# DB
# =========================================================
def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            turnover_value REAL,
            adjustment_factor REAL,
            adj_open REAL,
            adj_high REAL,
            adj_low REAL,
            adj_close REAL,
            adj_volume REAL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (code, date)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS master (
            code TEXT PRIMARY KEY,
            search_code TEXT NOT NULL,
            company_name TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fetch_log (
            chunk_start TEXT NOT NULL,
            chunk_end TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (chunk_start, chunk_end)
        )
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_code_date ON prices (code, date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices (date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_master_search_code ON master (search_code)")
    conn.commit()


def already_fetched(conn: sqlite3.Connection, chunk_start: str, chunk_end: str) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM fetch_log
        WHERE chunk_start = ? AND chunk_end = ?
        LIMIT 1
        """,
        (chunk_start, chunk_end),
    )
    return cur.fetchone() is not None


def mark_fetched(conn: sqlite3.Connection, chunk_start: str, chunk_end: str, row_count: int) -> None:
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO fetch_log (chunk_start, chunk_end, row_count, fetched_at)
            VALUES (?, ?, ?, ?)
            """,
            (chunk_start, chunk_end, row_count, now_jst_str()),
        )


# =========================================================
# J-Quants
# =========================================================
def get_client() -> jquantsapi.ClientV2:
    return jquantsapi.ClientV2(api_key=API_KEY)

def fetch_master_df(cli: jquantsapi.ClientV2) -> pd.DataFrame:
    df = None

    try:
        df = call_with_retry(cli.get_eq_master)
    except Exception:
        df = None

    if df is None or len(df) == 0:
        try:
            df = call_with_retry(cli.get_listed_info)
        except Exception:
            df = None

    if df is None or len(df) == 0:
        return pd.DataFrame()

    return df.copy()


def fetch_prices_by_chunk(cli: jquantsapi.ClientV2, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    df = call_with_retry(
        cli.get_eq_bars_daily_range,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    if df is None or len(df) == 0:
        return pd.DataFrame()

    work = df.copy()
    if "Date" in work.columns:
        work["Date"] = pd.to_datetime(work["Date"]).dt.strftime("%Y-%m-%d")
    return work


# =========================================================
# 保存
# =========================================================
def save_master(conn: sqlite3.Connection, master_df: pd.DataFrame) -> int:
    if master_df is None or master_df.empty:
        return 0

    work = master_df.copy()
    code_col = first_existing_col(work, ["Code", "code", "LocalCode", "local_code"])
    name_col = first_existing_col(
        work,
        ["CompanyName", "company_name", "Name", "name", "IssueName", "IssueNameJa"],
    )

    if code_col is None:
        print("[WARN] マスタ保存をスキップ: code列が見つかりません。")
        return 0

    rows = []
    updated_at = now_jst_str()

    for _, r in work.iterrows():
        code_raw = r.get(code_col)
        if pd.isna(code_raw):
            continue

        code = normalize_code(code_raw)
        company_name = ""
        if name_col and pd.notna(r.get(name_col)):
            company_name = str(r.get(name_col)).strip()

        rows.append((code, code, company_name, updated_at))

    with conn:
        conn.executemany(
            """
            INSERT INTO master (code, search_code, company_name, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                search_code=excluded.search_code,
                company_name=excluded.company_name,
                updated_at=excluded.updated_at
            """,
            rows,
        )

    return len(rows)


def save_prices(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    work = df.copy()

    code_col = first_existing_col(work, ["Code", "code", "LocalCode", "local_code"])
    date_col = first_existing_col(work, ["Date", "date"])

    if code_col is None or date_col is None:
        print("[WARN] prices保存をスキップ: Code/Date列が見つかりません。")
        return 0

    open_col = first_existing_col(work, ["O", "Open", "open"])
    high_col = first_existing_col(work, ["H", "High", "high"])
    low_col = first_existing_col(work, ["L", "Low", "low"])
    close_col = first_existing_col(work, ["C", "Close", "close"])
    volume_col = first_existing_col(work, ["V", "Volume", "volume"])
    turnover_col = first_existing_col(work, ["TurnoverValue", "turnover_value"])
    adj_factor_col = first_existing_col(work, ["AdjustmentFactor", "AdjFactor", "adjustment_factor"])
    adj_open_col = first_existing_col(work, ["AdjO", "AdjustmentOpen", "adj_open"])
    adj_high_col = first_existing_col(work, ["AdjH", "AdjustmentHigh", "adj_high"])
    adj_low_col = first_existing_col(work, ["AdjL", "AdjustmentLow", "adj_low"])
    adj_close_col = first_existing_col(work, ["AdjC", "AdjustmentClose", "adj_close"])
    adj_volume_col = first_existing_col(work, ["AdjVo", "AdjustmentVolume", "adj_volume"])

    fetched_at = now_jst_str()
    rows = []

    for _, r in work.iterrows():
        rows.append(
            (
                normalize_code(r.get(code_col)),
                str(r.get(date_col)).strip(),
                to_float(r.get(open_col)) if open_col else None,
                to_float(r.get(high_col)) if high_col else None,
                to_float(r.get(low_col)) if low_col else None,
                to_float(r.get(close_col)) if close_col else None,
                to_float(r.get(volume_col)) if volume_col else None,
                to_float(r.get(turnover_col)) if turnover_col else None,
                to_float(r.get(adj_factor_col)) if adj_factor_col else None,
                to_float(r.get(adj_open_col)) if adj_open_col else None,
                to_float(r.get(adj_high_col)) if adj_high_col else None,
                to_float(r.get(adj_low_col)) if adj_low_col else None,
                to_float(r.get(adj_close_col)) if adj_close_col else None,
                to_float(r.get(adj_volume_col)) if adj_volume_col else None,
                fetched_at,
            )
        )

    with conn:
        conn.executemany(
            """
            INSERT INTO prices (
                code, date, open, high, low, close, volume,
                turnover_value, adjustment_factor,
                adj_open, adj_high, adj_low, adj_close, adj_volume,
                fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code, date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                turnover_value=excluded.turnover_value,
                adjustment_factor=excluded.adjustment_factor,
                adj_open=excluded.adj_open,
                adj_high=excluded.adj_high,
                adj_low=excluded.adj_low,
                adj_close=excluded.adj_close,
                adj_volume=excluded.adj_volume,
                fetched_at=excluded.fetched_at
            """,
            rows,
        )

    return len(rows)


# =========================================================
# 期間
# =========================================================
def date_chunks(start_date: datetime, end_date: datetime, days: int):
    current = start_date
    while current <= end_date:
        chunk_end = current + timedelta(days=days - 1)
        if chunk_end > end_date:
            chunk_end = end_date
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


# =========================================================
# メイン
# =========================================================
def main():
    print("=== J-Quants 5年分SQLite保存スクリプト（429対策版） ===")

    cli = get_client()
    conn = get_conn(DB_PATH)
    init_db(conn)

    print("[1/3] 銘柄マスタ取得中...")
    master_df = fetch_master_df(cli)
    master_count = save_master(conn, master_df)
    print(f"[OK] master 保存件数: {master_count:,}")

    today = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = (today - relativedelta(years=YEARS_BACK)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today

    print(f"[2/3] 株価日足取得開始: {start_date.date()} ～ {end_date.date()}")

    total_rows = 0

    for chunk_start, chunk_end in date_chunks(start_date, end_date, CHUNK_DAYS):
        s = chunk_start.strftime("%Y-%m-%d")
        e = chunk_end.strftime("%Y-%m-%d")

        if already_fetched(conn, s, e):
            print(f"[SKIP] {s} ～ {e} は取得済み")
            continue

        print(f"[FETCH] {s} ～ {e}")

        df_chunk = fetch_prices_by_chunk(cli, chunk_start, chunk_end)
        row_count = save_prices(conn, df_chunk)
        mark_fetched(conn, s, e, row_count)

        total_rows += row_count
        print(f"[OK] {s} ～ {e} 保存件数: {row_count:,}")

        print(f"[INFO] {CHUNK_SLEEP_SECONDS}秒休みます。")
        time.sleep(CHUNK_SLEEP_SECONDS)

    print(f"[3/3] 完了。今回保存した件数: {total_rows:,}")

    cur = conn.execute("SELECT COUNT(*) FROM prices")
    price_total = cur.fetchone()[0]

    cur = conn.execute("SELECT COUNT(*) FROM master")
    master_total = cur.fetchone()[0]

    print("=== DBサマリー ===")
    print(f"prices 件数 : {price_total:,}")
    print(f"master 件数 : {master_total:,}")
    print(f"DBファイル : {DB_PATH}")

    conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] 中断しました。")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        sys.exit(1)