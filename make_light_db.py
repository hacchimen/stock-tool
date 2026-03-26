import sqlite3
import os
from datetime import datetime, timedelta

SRC = "data/jquants_prices.db"
DST = "data/jquants_prices_light.db"

# 既存ファイル削除
if os.path.exists(DST):
    os.remove(DST)

# 直近1年（必要なら730に変更）
from_date = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
print(f"{from_date} 以降を抽出")

conn = sqlite3.connect(DST)

# 元DBをアタッチ
conn.execute(f"ATTACH DATABASE '{SRC}' AS src")

# --- pricesテーブル（必要列だけ）---
conn.execute("""
CREATE TABLE prices AS
SELECT
    code,
    date,
    open,
    high,
    low,
    close,
    volume,
    adjustment_factor,
    adj_close
FROM src.prices
WHERE date >= ?
""", (from_date,))

# --- masterテーブル（そのままコピー）---
conn.execute("""
CREATE TABLE master AS
SELECT *
FROM src.master
""")

# インデックス（高速化）
conn.execute("CREATE INDEX idx_prices_code_date ON prices(code, date)")
conn.execute("CREATE INDEX idx_master_code ON master(code)")
conn.execute("CREATE INDEX idx_master_search_code ON master(search_code)")

conn.commit()

# サイズ圧縮
conn.execute("VACUUM")

# 確認
cnt = conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
print(f"prices件数: {cnt:,}")

tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print("テーブル一覧:", tables)

conn.close()

print("軽量DB完成（master込み）")