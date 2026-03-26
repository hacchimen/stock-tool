import io
import os
import sqlite3

import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfgen import canvas


DB_PATH = os.path.join("data", "jquants_prices_light.db")


st.set_page_config(
    page_title="株価評価ツール",
    layout="centered",
)

st.markdown(
    """
    <style>
    .stApp {
        background-color: #f7f8fa;
    }

    .main .block-container {
        max-width: 900px;
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }

    h1, h2, h3 {
        color: #1f2d3d;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def normalize_code(value: str) -> str:
    return str(value).strip().upper()


def normalize_search_code(value: str) -> str:
    s = normalize_code(value)
    if s.endswith("0"):
        return s[:-1]
    return s


def get_conn():
    return sqlite3.connect(DB_PATH)


def round_price_for_display(value):
    if value is None or pd.isna(value):
        return None
    return round(float(value), 2)


def fmt_price(value):
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):,.2f}円"


def fmt_date(value):
    if value is None or value == "":
        return "-"
    return str(value)


@st.cache_data(ttl=300)
def get_company_name(code: str) -> str:
    search_code = normalize_search_code(code)
    conn = get_conn()
    try:
        sql = """
            SELECT company_name
            FROM master
            WHERE UPPER(search_code) = ?
               OR UPPER(code) = ?
               OR UPPER(code) LIKE ?
            ORDER BY code
            LIMIT 1
        """
        df = pd.read_sql_query(
            sql,
            conn,
            params=[search_code, search_code, search_code + "%"],
        )
        if df.empty:
            return ""
        value = df.iloc[0]["company_name"]
        return str(value).strip() if pd.notna(value) else ""
    finally:
        conn.close()


@st.cache_data(ttl=300)
def get_price_df_from_db(code: str, base_date: str) -> pd.DataFrame:
    search_code = normalize_search_code(code)
    base_dt = pd.to_datetime(base_date)

    start_dt = (base_dt.replace(day=1) - relativedelta(months=2)).strftime("%Y-%m-%d")
    month_end = (base_dt.replace(day=1) + relativedelta(months=1) - relativedelta(days=1))
    end_dt = (month_end + relativedelta(days=7)).strftime("%Y-%m-%d")

    conn = get_conn()
    try:
        sql = """
            SELECT
                p.code,
                p.date AS Date,
                p.open AS O,
                p.high AS H,
                p.low AS L,
                p.close AS C,
                p.volume AS V,
                p.adjustment_factor AS AdjustmentFactor,
                p.adj_close AS AdjC
            FROM prices p
            JOIN master m
              ON p.code = m.code
            WHERE (
                    UPPER(m.search_code) = ?
                 OR UPPER(m.code) = ?
                 OR UPPER(m.code) LIKE ?
                  )
              AND p.date BETWEEN ? AND ?
            ORDER BY p.date
        """
        df = pd.read_sql_query(
            sql,
            conn,
            params=[search_code, search_code, search_code + "%", start_dt, end_dt],
        )
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame()

    df["Date"] = pd.to_datetime(df["Date"])
    return df


def get_month_average(df: pd.DataFrame, year: int, month: int, price_col: str = "C"):
    if df is None or df.empty:
        return None

    target = df[(df["Date"].dt.year == year) & (df["Date"].dt.month == month)].copy()
    if target.empty:
        return None

    avg_value = float(target[price_col].mean())
    return round_price_for_display(avg_value)


def detect_split_alert(df: pd.DataFrame, base_date: str) -> dict:
    empty_result = {
        "has_alert": False,
        "message": "",
        "detected_dates": [],
        "detected_factors": [],
    }

    if df is None or df.empty or "AdjustmentFactor" not in df.columns:
        return empty_result

    work = df.copy()
    work["Date"] = pd.to_datetime(work["Date"])
    base_dt = pd.to_datetime(base_date).normalize()
    from_dt = (base_dt - relativedelta(months=3)).normalize()

    target = work[(work["Date"] >= from_dt) & (work["Date"] <= base_dt)].copy()
    if target.empty:
        return empty_result

    target["AdjustmentFactor"] = pd.to_numeric(target["AdjustmentFactor"], errors="coerce")
    detected = target[target["AdjustmentFactor"].notna() & (target["AdjustmentFactor"] != 1)].copy()

    if detected.empty:
        return empty_result

    return {
        "has_alert": True,
        "message": "評価基準日前3か月以内に株式分割・株式併合の可能性があります。",
        "detected_dates": detected["Date"].dt.strftime("%Y-%m-%d").tolist(),
        "detected_factors": [float(x) for x in detected["AdjustmentFactor"].tolist()],
    }


def get_inheritance_valuation_close(df: pd.DataFrame, base_date: str, price_col: str = "C"):
    if df is None or df.empty:
        return None

    work = df.copy()
    work["Date"] = pd.to_datetime(work["Date"])
    work = work.sort_values("Date").reset_index(drop=True)
    base_dt = pd.to_datetime(base_date).normalize()

    same_day = work[work["Date"].dt.normalize() == base_dt]
    if not same_day.empty:
        row = same_day.iloc[-1]
        price = round_price_for_display(row[price_col])
        return {
            "price": price,
            "method": "評価基準日の終値",
            "base_date": str(base_dt.date()),
            "prev_date": str(row["Date"].date()),
            "next_date": str(row["Date"].date()),
            "prev_price": price,
            "next_price": price,
        }

    prev_df = work[work["Date"] < base_dt]
    next_df = work[work["Date"] > base_dt]

    prev_row = prev_df.iloc[-1] if not prev_df.empty else None
    next_row = next_df.iloc[0] if not next_df.empty else None

    if prev_row is None and next_row is None:
        return None

    if prev_row is None:
        next_price = round_price_for_display(next_row[price_col])
        return {
            "price": next_price,
            "method": "後営業日の終値",
            "base_date": str(base_dt.date()),
            "prev_date": None,
            "next_date": str(next_row["Date"].date()),
            "prev_price": None,
            "next_price": next_price,
        }

    if next_row is None:
        prev_price = round_price_for_display(prev_row[price_col])
        return {
            "price": prev_price,
            "method": "前営業日の終値",
            "base_date": str(base_dt.date()),
            "prev_date": str(prev_row["Date"].date()),
            "next_date": None,
            "prev_price": prev_price,
            "next_price": None,
        }

    prev_date = pd.to_datetime(prev_row["Date"]).normalize()
    next_date = pd.to_datetime(next_row["Date"]).normalize()

    prev_diff = (base_dt - prev_date).days
    next_diff = (next_date - base_dt).days

    prev_price_raw = float(prev_row[price_col])
    next_price_raw = float(next_row[price_col])

    prev_price = round_price_for_display(prev_price_raw)
    next_price = round_price_for_display(next_price_raw)

    if prev_diff < next_diff:
        return {
            "price": prev_price,
            "method": "前営業日の終値",
            "base_date": str(base_dt.date()),
            "prev_date": str(prev_row["Date"].date()),
            "next_date": str(next_row["Date"].date()),
            "prev_price": prev_price,
            "next_price": next_price,
        }

    if next_diff < prev_diff:
        return {
            "price": next_price,
            "method": "後営業日の終値",
            "base_date": str(base_dt.date()),
            "prev_date": str(prev_row["Date"].date()),
            "next_date": str(next_row["Date"].date()),
            "prev_price": prev_price,
            "next_price": next_price,
        }

    avg_price = round_price_for_display((prev_price_raw + next_price_raw) / 2)
    return {
        "price": avg_price,
        "method": "前後営業日の終値平均",
        "base_date": str(base_dt.date()),
        "prev_date": str(prev_row["Date"].date()),
        "next_date": str(next_row["Date"].date()),
        "prev_price": prev_price,
        "next_price": next_price,
    }


def evaluate_stock_price(df: pd.DataFrame, base_date: str):
    if df is None or df.empty:
        return {"error": "株価データがありません"}

    work = df.copy()
    work["Date"] = pd.to_datetime(work["Date"])
    work = work.sort_values("Date").reset_index(drop=True)
    base_dt = pd.to_datetime(base_date)

    close_info = get_inheritance_valuation_close(work, base_date, price_col="C")
    if close_info is None:
        return {"error": "評価基準日の終値が取得できません"}

    ym0 = base_dt
    ym1 = base_dt - relativedelta(months=1)
    ym2 = base_dt - relativedelta(months=2)

    avg_0 = get_month_average(work, ym0.year, ym0.month, price_col="C")
    avg_1 = get_month_average(work, ym1.year, ym1.month, price_col="C")
    avg_2 = get_month_average(work, ym2.year, ym2.month, price_col="C")

    candidates = {
        close_info["method"]: close_info["price"],
        f"{ym0.year}年{ym0.month}月平均": avg_0,
        f"{ym1.year}年{ym1.month}月平均": avg_1,
        f"{ym2.year}年{ym2.month}月平均": avg_2,
    }

    valid_candidates = {k: v for k, v in candidates.items() if v is not None}
    if not valid_candidates:
        return {"error": "判定可能な候補がありません"}

    adopted_method = min(valid_candidates, key=valid_candidates.get)
    adopted_price = round_price_for_display(valid_candidates[adopted_method])
    split_alert = detect_split_alert(work, base_date)

    return {
        "base_date": str(base_dt.date()),
        "close_info": close_info,
        "candidates": valid_candidates,
        "adopted_method": adopted_method,
        "adopted_price": adopted_price,
        "split_alert": split_alert,
    }


def build_copy_text(code: str, company_name: str, result: dict) -> str:
    close_info = result["close_info"]
    split_alert = result["split_alert"]

    lines = [
        "【株価評価結果】",
        f"銘柄コード：{code}",
        f"銘柄名：{company_name or '-'}",
        f"評価基準日：{result['base_date']}",
        "",
        "■ 基準日終値判定",
        f"判定方法：{close_info['method']}",
        f"基準日終値：{fmt_price(close_info['price'])}",
        f"前営業日：{fmt_date(close_info['prev_date'])}",
        f"後営業日：{fmt_date(close_info['next_date'])}",
    ]

    if close_info["method"] == "前後営業日の終値平均":
        lines.extend([
            f"前営業日終値：{fmt_price(close_info['prev_price'])}",
            f"後営業日終値：{fmt_price(close_info['next_price'])}",
        ])

    lines.extend(["", "■ 候補一覧"])
    for k, v in result["candidates"].items():
        lines.append(f"{k}：{fmt_price(v)}")

    lines.extend([
        "",
        "■ 最終判定",
        f"採用方法：{result['adopted_method']}",
        f"採用株価：{fmt_price(result['adopted_price'])}",
        "",
        "■ 株式分割・併合アラート",
    ])

    if split_alert["has_alert"]:
        lines.append("あり")
        lines.append(split_alert["message"])
        lines.append(f"検知日：{', '.join(split_alert['detected_dates'])}")
        lines.append(
            "検知Factor："
            + ", ".join([str(round_price_for_display(x)) for x in split_alert["detected_factors"]])
        )
    else:
        lines.append("なし")

    return "\n".join(lines)


def build_result_dataframe(code: str, company_name: str, result: dict) -> pd.DataFrame:
    close_info = result["close_info"]
    split_alert = result["split_alert"]

    row = {
        "銘柄コード": code,
        "銘柄名": company_name,
        "評価基準日": result["base_date"],
        "基準日終値判定方法": close_info["method"],
        "基準日終値": close_info["price"],
        "前営業日": close_info["prev_date"],
        "後営業日": close_info["next_date"],
        "前営業日終値": close_info["prev_price"],
        "後営業日終値": close_info["next_price"],
        "最終採用方法": result["adopted_method"],
        "最終採用株価": result["adopted_price"],
        "株式分割・併合アラート": "あり" if split_alert["has_alert"] else "なし",
        "アラート内容": split_alert["message"],
        "検知日": ", ".join(split_alert["detected_dates"]),
        "検知Factor": ", ".join([str(round_price_for_display(x)) for x in split_alert["detected_factors"]]),
    }

    for k, v in result["candidates"].items():
        row[k] = v

    return pd.DataFrame([row])


def build_excel_bytes(summary_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="評価結果")
    output.seek(0)
    return output.read()


def build_pdf_bytes(report_text: str) -> bytes:
    pdfmetrics.registerFont(UnicodeCIDFont("HeiseiKakuGo-W5"))
    output = io.BytesIO()
    c = canvas.Canvas(output, pagesize=A4)
    _, height = A4

    c.setTitle("株価評価結果")
    c.setFont("HeiseiKakuGo-W5", 11)

    x = 40
    y = height - 40
    line_height = 16

    for line in report_text.split("\n"):
        if y < 40:
            c.showPage()
            c.setFont("HeiseiKakuGo-W5", 11)
            y = height - 40
        c.drawString(x, y, line)
        y -= line_height

    c.save()
    output.seek(0)
    return output.read()


st.title("株価評価ツール")
st.caption("相続税評価業務向け / 保存済み株価データ参照版")
st.caption("免責事項・利用規約は左サイドバーの各ページをご確認ください。")

if not os.path.exists(DB_PATH):
    st.error("DBファイルがありません。先に build_jquants_db.py を実行してください。")
    st.stop()

st.subheader("入力")
code = st.text_input("銘柄コード", value="7203").strip()
base_date = st.date_input("評価基準日")
evaluate_clicked = st.button("評価する", use_container_width=True)

if evaluate_clicked:
    if not code:
        st.error("銘柄コードを入力してください。")
    else:
        company_name = get_company_name(code)

        with st.spinner("保存済みデータから評価中..."):
            df = get_price_df_from_db(code, str(base_date))

        if df.empty:
            st.error("保存済みDBに必要な株価データがありません。DB更新後に再度お試しください。")
        else:
            result = evaluate_stock_price(df, str(base_date))

            if "error" in result:
                st.error(result["error"])
            else:
                close_info = result["close_info"]
                split_alert = result["split_alert"]

                if split_alert["has_alert"]:
                    st.warning(split_alert["message"])
                    st.write(f"検知日: {', '.join(split_alert['detected_dates'])}")
                    st.write(
                        "検知Factor: "
                        + ", ".join([str(round_price_for_display(x)) for x in split_alert["detected_factors"]])
                    )

                st.subheader("評価結果")
                c1, c2 = st.columns(2)
                with c1:
                    st.metric("最終採用方法", result["adopted_method"])
                with c2:
                    st.metric("最終採用株価", fmt_price(result["adopted_price"]))

                st.write(f"銘柄コード: {code}")
                if company_name:
                    st.write(f"銘柄名: {company_name}")
                st.write(f"評価基準日: {result['base_date']}")
                st.write(f"基準日終値の判定方法: {close_info['method']}")
                st.write(f"基準日終値（採用値）: {fmt_price(close_info['price'])}")
                st.write(f"前営業日: {fmt_date(close_info['prev_date'])}")
                st.write(f"後営業日: {fmt_date(close_info['next_date'])}")

                if close_info["method"] == "前後営業日の終値平均":
                    st.write(f"前営業日終値: {fmt_price(close_info['prev_price'])}")
                    st.write(f"後営業日終値: {fmt_price(close_info['next_price'])}")

                st.subheader("候補一覧")
                candidate_df = pd.DataFrame(
                    [{"項目": k, "金額": fmt_price(v)} for k, v in result["candidates"].items()]
                )
                st.dataframe(candidate_df, use_container_width=True, hide_index=True)

                copy_text = build_copy_text(code, company_name, result)

                st.subheader("コピペ用テキスト")
                st.text_area("内容", value=copy_text, height=260)
                st.download_button(
                    label="コピペ用テキストをダウンロード",
                    data=copy_text.encode("utf-8-sig"),
                    file_name=f"stock_valuation_copy_{code}_{result['base_date']}.txt",
                    mime="text/plain",
                    use_container_width=True,
                )

                summary_df = build_result_dataframe(code, company_name, result)
                pdf_bytes = build_pdf_bytes(copy_text)
                excel_bytes = build_excel_bytes(summary_df)

                st.subheader("出力")
                o1, o2 = st.columns(2)
                with o1:
                    st.download_button(
                        label="PDF出力",
                        data=pdf_bytes,
                        file_name=f"stock_valuation_{code}_{result['base_date']}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                with o2:
                    st.download_button(
                        label="Excel出力",
                        data=excel_bytes,
                        file_name=f"stock_valuation_{code}_{result['base_date']}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )