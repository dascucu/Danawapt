import os
import streamlit as st
import pandas as pd
import requests
import urllib3
import xml.etree.ElementTree as ET
import time
import datetime
import plotly.graph_objects as go
from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── 기본 설정 ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="아파트 시세 비교 (다나와)",
    page_icon="🏢",
    layout="wide",
)

API_KEY = st.secrets.get("API_KEY", os.getenv("API_KEY", ""))
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
MAX_COMPARE = 5  # 최대 비교 아파트 수

COLORS = ["#4C72B0", "#DD8452", "#55A868", "#C44E52", "#8172B3"]

# ── 데이터 로드 ────────────────────────────────────────────────────────────────
@st.cache_data
def load_static_data():
    apt_df = pd.read_csv("apt_list.csv", encoding="utf-8")
    apt_df["세대수"] = apt_df.get("세대수", pd.Series([0] * len(apt_df))).fillna(0).astype(int)
    apt_df["연식"] = (
        apt_df.get("사용승인일", apt_df.get("건축년도", pd.Series([""] * len(apt_df))))
        .astype(str)
        .str[:4]
    )

    region_df = pd.read_csv("region_code.csv", encoding="utf-8")
    region_map = {}
    for _, row in region_df.iterrows():
        code_str = str(row["법정동코드"])
        name_str = str(row["법정동명"])
        if len(code_str) >= 5 and " " in name_str:
            key = code_str[:5]
            if key not in region_map:
                region_map[key] = name_str
    return apt_df, region_map


apt_df, region_map = load_static_data()
region_options = {v: k for k, v in region_map.items()}
sorted_regions = sorted(region_options.keys())

# ── 세션 상태 초기화 ───────────────────────────────────────────────────────────
if "compare_list" not in st.session_state:
    st.session_state["compare_list"] = []  # [{"label": ..., "lawd_cd": ..., "apt_name": ...}, ...]
if "compare_results" not in st.session_state:
    st.session_state["compare_results"] = {}  # label → DataFrame


# ── 유틸 함수 ──────────────────────────────────────────────────────────────────
_EMPTY_ROW_EXTRA = {
    "평균_전용면적": None, "평균_평당가": None, "직거래수": 0, "전월대비_등락": None
}


def fetch_monthly_data(lawd_cd: str, apt_name: str, start_ym: str, end_ym: str):
    """API를 호출해 월별 거래 통계를 DataFrame으로 반환."""
    cur_y, cur_m = int(start_ym[:4]), int(start_ym[4:])
    end_y, end_m = int(end_ym[:4]), int(end_ym[4:])
    rows = []

    while True:
        deal_ymd = f"{cur_y}{cur_m:02d}"
        url = (
            f"{API_URL}?serviceKey={API_KEY}"
            f"&pageNo=1&numOfRows=1000"
            f"&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}"
        )
        try:
            time.sleep(0.08)
            res = requests.get(url, timeout=10, verify=False)
            if res.status_code == 200:
                root = ET.fromstring(res.content)
                header = root.find("header")
                if header is not None and header.findtext("resultCode") == "000":
                    transactions = []
                    for item in root.findall(".//item"):
                        name = (item.findtext("aptNm") or "").strip()
                        if name == apt_name.strip():
                            price_str = (item.findtext("dealAmount") or "0").replace(",", "").strip()
                            try:
                                p = int(price_str)
                                if p <= 0:
                                    continue
                            except ValueError:
                                continue
                            # 전용면적
                            try:
                                area = float((item.findtext("excluUseAr") or "").strip())
                                area = area if area > 0 else None
                            except (ValueError, TypeError):
                                area = None
                            # 층
                            try:
                                floor_val = int((item.findtext("floor") or "").strip())
                            except (ValueError, TypeError):
                                floor_val = None
                            gbn = (item.findtext("dealingGbn") or "").strip()
                            transactions.append({
                                "price": p, "area": area,
                                "floor": floor_val, "dealingGbn": gbn,
                            })

                    if transactions:
                        prices = [t["price"] for t in transactions]
                        areas = [t["area"] for t in transactions if t["area"] is not None]
                        ppyg_vals = [
                            t["price"] / (t["area"] / 3.3058)
                            for t in transactions
                            if t["area"] and t["area"] > 0
                        ]
                        direct = sum(1 for t in transactions if "직거래" in t["dealingGbn"])
                        rows.append({
                            "거래년월": f"{deal_ymd[:4]}-{deal_ymd[4:]}",
                            "평균가": round(sum(prices) / len(prices)),
                            "최저가": min(prices),
                            "최고가": max(prices),
                            "거래건수": len(prices),
                            "평균_전용면적": round(sum(areas) / len(areas), 1) if areas else None,
                            "평균_평당가": round(sum(ppyg_vals) / len(ppyg_vals)) if ppyg_vals else None,
                            "직거래수": direct,
                            "전월대비_등락": None,  # 후처리에서 채움
                        })
                    else:
                        rows.append({
                            "거래년월": f"{deal_ymd[:4]}-{deal_ymd[4:]}",
                            "평균가": None, "최저가": None, "최고가": None, "거래건수": 0,
                            **_EMPTY_ROW_EXTRA,
                        })
        except Exception:
            rows.append({
                "거래년월": f"{deal_ymd[:4]}-{deal_ymd[4:]}",
                "평균가": None, "최저가": None, "최고가": None, "거래건수": -1,
                **_EMPTY_ROW_EXTRA,
            })

        if cur_y == end_y and cur_m == end_m:
            break
        cur_m += 1
        if cur_m > 12:
            cur_m = 1
            cur_y += 1

    # 전월대비 등락률 후처리
    result_df = pd.DataFrame(rows)
    prev = None
    mom = []
    for _, r in result_df.iterrows():
        cur_avg = r["평균가"]
        if prev is not None and prev > 0 and cur_avg is not None and cur_avg > 0:
            mom.append(round((cur_avg - prev) / prev * 100, 2))
        else:
            mom.append(None)
        if cur_avg is not None and cur_avg > 0:
            prev = cur_avg
    result_df["전월대비_등락"] = mom
    return result_df


def format_price(val):
    """만원 → 억/만원 표시."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "-"
    val = int(val)
    if val >= 10000:
        eok = val // 10000
        rem = val % 10000
        return f"{eok}억 {rem:,}만" if rem else f"{eok}억"
    return f"{val:,}만"


# ── 레이아웃 ───────────────────────────────────────────────────────────────────
st.title("🏢 아파트 시세 비교")
st.caption("최대 5개 아파트를 담아 가격 추이를 한눈에 비교하세요.")
st.markdown("---")

# 좌측: 아파트 담기 / 우측: 비교 장바구니
left_col, right_col = st.columns([1, 2])

# ══ 왼쪽 패널 ══════════════════════════════════════════════════════════════════
with left_col:
    st.subheader("🔍 아파트 찾기")

    sel_region = st.selectbox(
        "지역 선택",
        ["지역을 선택하세요"] + sorted_regions,
        key="sel_region",
    )

    sel_apt_display = None
    if sel_region != "지역을 선택하세요":
        lawd_cd = region_options[sel_region]
        filtered = apt_df[apt_df["주소"].str.contains(sel_region, na=False)]
        filtered = filtered[filtered["세대수"] >= 100].sort_values("세대수", ascending=False)
        filtered = filtered.drop_duplicates(subset=["단지명_공시가격"])

        display_map = {}
        for _, row in filtered.iterrows():
            orig = row["단지명_공시가격"]
            label = f"{orig} ({row['세대수']}세대, {row['연식']}년)"
            display_map[label] = orig

        if not display_map:
            st.warning("해당 지역에 아파트 정보가 없습니다.")
        else:
            sel_apt_display = st.selectbox(
                "아파트 선택",
                ["아파트를 선택하세요"] + list(display_map.keys()),
                key="sel_apt",
            )

    # 담기 버튼
    if sel_apt_display and sel_apt_display != "아파트를 선택하세요":
        orig_name = display_map[sel_apt_display]
        label = f"{sel_region} · {orig_name}"
        already_in = any(item["label"] == label for item in st.session_state["compare_list"])
        full = len(st.session_state["compare_list"]) >= MAX_COMPARE

        if already_in:
            st.info("이미 비교 목록에 있습니다.")
        elif full:
            st.warning(f"최대 {MAX_COMPARE}개까지 비교할 수 있습니다.")
        else:
            if st.button("➕ 비교 목록에 담기", use_container_width=True):
                st.session_state["compare_list"].append({
                    "label": label,
                    "lawd_cd": region_options[sel_region],
                    "apt_name": orig_name,
                    "region": sel_region,
                })
                st.rerun()

    st.markdown("---")
    st.subheader("📅 조회 기간")
    col_s, col_e = st.columns(2)
    with col_s:
        start_date = st.date_input("시작월", datetime.date(2023, 1, 1), key="start_dt")
    with col_e:
        end_date = st.date_input("종료월", datetime.date.today().replace(day=1), key="end_dt")

    if end_date < start_date:
        st.error("종료월이 시작월보다 앞설 수 없습니다.")
    else:
        st.markdown("")
        run_btn = st.button(
            "📊 비교 시작",
            use_container_width=True,
            type="primary",
            disabled=len(st.session_state["compare_list"]) == 0,
        )

# ══ 오른쪽 패널: 비교 장바구니 ══════════════════════════════════════════════════
with right_col:
    st.subheader(f"🛒 비교 목록 ({len(st.session_state['compare_list'])}/{MAX_COMPARE})")

    if not st.session_state["compare_list"]:
        st.info("왼쪽에서 아파트를 검색해 비교 목록에 담아주세요.")
    else:
        for i, item in enumerate(st.session_state["compare_list"]):
            color = COLORS[i % len(COLORS)]
            c1, c2 = st.columns([6, 1])
            with c1:
                st.markdown(
                    f"<span style='color:{color}; font-size:18px;'>●</span> "
                    f"**{item['label']}**",
                    unsafe_allow_html=True,
                )
            with c2:
                if st.button("✕", key=f"del_{i}"):
                    st.session_state["compare_list"].pop(i)
                    # 해당 결과도 제거
                    st.session_state["compare_results"].pop(item["label"], None)
                    st.rerun()

        if st.button("🗑️ 전체 초기화", use_container_width=True):
            st.session_state["compare_list"] = []
            st.session_state["compare_results"] = {}
            st.rerun()

# ── 비교 실행 ──────────────────────────────────────────────────────────────────
if "run_btn" in dir() and run_btn and st.session_state["compare_list"]:
    start_ym = start_date.strftime("%Y%m")
    end_ym = end_date.strftime("%Y%m")
    total = len(st.session_state["compare_list"])

    progress = st.progress(0, text="데이터를 가져오는 중...")
    for idx, item in enumerate(st.session_state["compare_list"]):
        progress.progress((idx) / total, text=f"[{idx+1}/{total}] {item['label']} 조회 중...")
        df = fetch_monthly_data(item["lawd_cd"], item["apt_name"], start_ym, end_ym)
        st.session_state["compare_results"][item["label"]] = df
    progress.progress(1.0, text="완료!")
    time.sleep(0.4)
    progress.empty()

# ── 결과 출력 ──────────────────────────────────────────────────────────────────
if st.session_state["compare_results"]:
    st.markdown("---")

    valid_labels = [item["label"] for item in st.session_state["compare_list"]
                    if item["label"] in st.session_state["compare_results"]]

    # Y축 지표 토글
    hdr_col, toggle_col = st.columns([3, 1])
    with hdr_col:
        st.subheader("📈 거래가 추이 비교")
    with toggle_col:
        chart_metric = st.radio(
            "Y축 지표",
            ["총 거래가", "평당가"],
            horizontal=True,
            key="chart_metric",
        )
    y_col   = "평균가" if chart_metric == "총 거래가" else "평균_평당가"
    y_title = "평균 거래가 (만원)" if chart_metric == "총 거래가" else "평균 평당가 (만원/평)"
    hover_label = "평균가" if chart_metric == "총 거래가" else "평당가"

    # Plotly 복합 라인 차트
    fig = go.Figure()
    for i, label in enumerate(valid_labels):
        df = st.session_state["compare_results"][label]
        chart_df = df[df[y_col].notna() & (df["거래건수"] > 0)]
        color = COLORS[i % len(COLORS)]
        fig.add_trace(go.Scatter(
            x=chart_df["거래년월"],
            y=chart_df[y_col],
            mode="lines+markers",
            name=label,
            line=dict(color=color, width=2.5),
            marker=dict(size=6),
            hovertemplate=(
                f"<b>%{{fullData.name}}</b><br>"
                f"거래년월: %{{x}}<br>"
                f"{hover_label}: %{{customdata}}<extra></extra>"
            ),
            customdata=[format_price(v) for v in chart_df[y_col]],
        ))

    fig.update_layout(
        xaxis_title="거래년월",
        yaxis_title=y_title,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        height=450,
        margin=dict(l=10, r=10, t=40, b=10),
        yaxis=dict(tickformat=","),
    )
    st.plotly_chart(fig, use_container_width=True)

    # 거래량 바 차트 (토글)
    if st.checkbox("📊 거래량 바 차트 표시", key="show_volume"):
        vol_fig = go.Figure()
        for i, label in enumerate(valid_labels):
            df = st.session_state["compare_results"][label]
            vol_df = df[df["거래건수"] > 0]
            vol_fig.add_trace(go.Bar(
                x=vol_df["거래년월"],
                y=vol_df["거래건수"],
                name=label,
                marker_color=COLORS[i % len(COLORS)],
                opacity=0.8,
            ))
        vol_fig.update_layout(
            barmode="group",
            xaxis_title="거래년월",
            yaxis_title="거래건수 (건)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=280,
            margin=dict(l=10, r=10, t=30, b=10),
        )
        st.plotly_chart(vol_fig, use_container_width=True)

    # ── 요약 카드 ─────────────────────────────────────────────────────────────
    st.subheader("📋 요약 통계")
    summary_rows = []
    for i, label in enumerate(valid_labels):
        df = st.session_state["compare_results"][label]
        valid = df[df["평균가"].notna() & (df["거래건수"] > 0)]
        if valid.empty:
            summary_rows.append({
                "아파트": label,
                "최근 평균가": "-", "기간 최저가": "-", "기간 최고가": "-",
                "평균 전용면적": "-", "평균 평당가": "-",
                "누적 등락률": "-", "직거래 비율": "-",
                "총 거래건수": 0, "데이터 있는 월": 0,
            })
        else:
            latest = valid.iloc[-1]
            # 평균 전용면적
            area_vals = valid["평균_전용면적"].dropna()
            avg_area = f"{round(area_vals.mean(), 1)}㎡" if not area_vals.empty else "-"
            # 평균 평당가
            ppyg_vals = valid["평균_평당가"].dropna()
            avg_ppyg = format_price(round(ppyg_vals.mean())) if not ppyg_vals.empty else "-"
            # 누적 등락률
            first_p = valid.iloc[0]["평균가"]
            last_p  = latest["평균가"]
            if first_p and first_p > 0 and last_p and last_p > 0:
                cum = round((last_p - first_p) / first_p * 100, 1)
                cum_str = f"+{cum}%" if cum >= 0 else f"{cum}%"
            else:
                cum_str = "-"
            # 직거래 비율
            total_cnt = valid["거래건수"].sum()
            direct_cnt = valid["직거래수"].sum() if "직거래수" in valid.columns else 0
            direct_str = f"{round(direct_cnt / total_cnt * 100, 1)}%" if total_cnt > 0 else "-"

            summary_rows.append({
                "아파트": label,
                "최근 평균가": format_price(latest["평균가"]),
                "기간 최저가": format_price(valid["최저가"].min()),
                "기간 최고가": format_price(valid["최고가"].max()),
                "평균 전용면적": avg_area,
                "평균 평당가": avg_ppyg,
                "누적 등락률": cum_str,
                "직거래 비율": direct_str,
                "총 거래건수": int(total_cnt),
                "데이터 있는 월": len(valid),
            })

    summary_df = pd.DataFrame(summary_rows)

    def highlight_row(row):
        idx = summary_df.index[summary_df["아파트"] == row["아파트"]].tolist()
        if idx:
            color = COLORS[idx[0] % len(COLORS)] + "22"  # 투명도 추가
            return [f"background-color: {color}"] * len(row)
        return [""] * len(row)

    st.dataframe(
        summary_df.style.apply(highlight_row, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    # ── 월별 상세 데이터 ───────────────────────────────────────────────────────
    with st.expander("📂 월별 상세 데이터 펼치기"):
        tabs = st.tabs([item["label"].split(" · ")[-1] for item in st.session_state["compare_list"]
                        if item["label"] in st.session_state["compare_results"]])
        for tab, label in zip(tabs, valid_labels):
            with tab:
                df = st.session_state["compare_results"][label].copy()
                df["평균가"] = df["평균가"].apply(lambda x: format_price(x) if pd.notna(x) else "-")
                df["최저가"] = df["최저가"].apply(lambda x: format_price(x) if pd.notna(x) else "-")
                df["최고가"] = df["최고가"].apply(lambda x: format_price(x) if pd.notna(x) else "-")
                df["거래건수"] = df["거래건수"].apply(lambda x: f"{int(x)}건" if x > 0 else ("없음" if x == 0 else "오류"))
                df["평균_전용면적"] = df["평균_전용면적"].apply(lambda x: f"{x}㎡" if pd.notna(x) else "-")
                df["평균_평당가"] = df["평균_평당가"].apply(lambda x: format_price(x) if pd.notna(x) else "-")
                df["전월대비_등락"] = df["전월대비_등락"].apply(
                    lambda x: (f"+{x}%" if x >= 0 else f"{x}%") if pd.notna(x) else "-"
                )
                df["직거래수"] = df["직거래수"].apply(
                    lambda x: f"{int(x)}건" if pd.notna(x) and int(x) > 0 else "-"
                )
                # 컬럼 순서 정렬
                col_order = ["거래년월", "평균가", "최저가", "최고가", "거래건수",
                             "평균_전용면적", "평균_평당가", "전월대비_등락", "직거래수"]
                df = df[[c for c in col_order if c in df.columns]]
                st.dataframe(df, use_container_width=True, hide_index=True)
