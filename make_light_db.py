import sqlite3
import os
from datetime import datetime, timedelta

SRC = "data/jquants_prices.db"
DST = "data/jquants_prices_light.db"

if os.path.exists(DST):
    os.remove(DST)

conn_src = sqlite3.connect(SRC)
conn_dst = sqlite3.connect(DST)

# 直近1年分
from_date = (datetime.today() - timedelta(days=365)).strftime("%Y%m%d")
print(f"{from_date} 以降を抽出")

# コピー先テーブル作成
conn_dst.execute("""
CREATE TABLE prices (
    code TEXT,
    date TEXT,
    close REAL
)
""")

# コピー元から取得
rows = conn_src.execute("""
SELECT code, date, close
FROM prices
WHERE date >= ?
""", (from_date,)).fetchall()

print(f"抽出予定件数: {len(rows):,}")

# コピー先へ挿入
conn_dst.executemany("""
INSERT INTO prices (code, date, close)
VALUES (?, ?, ?)
""", rows)

conn_dst.commit()

# 圧縮
conn_dst.execute("VACUUM")

cnt = conn_dst.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
print(f"件数: {cnt:,}")

conn_src.close()
conn_dst.close()

print("軽量DB完成")