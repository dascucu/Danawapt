import os
import streamlit as st
import pandas as pd
import requests
import urllib3
import xml.etree.ElementTree as ET
import time
import datetime
from dotenv import load_dotenv

load_dotenv()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. 기본 화면 설정 ---
st.set_page_config(page_title="나만의 부동산 대시보드", page_icon="🏢", layout="wide")

API_KEY = st.secrets.get("API_KEY", os.getenv("API_KEY", ""))
API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"

# --- 2. 데이터 불러오기 ---
@st.cache_data
def load_data():
    try:
        apt_df = pd.read_csv('apt_list.csv', encoding='utf-8')
        apt_df['세대수'] = apt_df.get('세대수', pd.Series([0]*len(apt_df))).fillna(0).astype(int)
        apt_df['건축년도'] = apt_df.get('사용승인일', apt_df.get('건축년도', pd.Series([0]*len(apt_df)))).fillna(0)
        apt_df['연식'] = apt_df['건축년도'].astype(str).str[:4]
        
        SUDO_PREFIX = ("11", "28", "41")  # 서울, 인천, 경기
        region_df = pd.read_csv('region_code.csv', encoding='utf-8')
        region_map = {str(row['법정동코드'])[:5]: str(row['법정동명']) for _, row in region_df.iterrows() if len(str(row['법정동코드'])) >= 5 and " " in str(row['법정동명']) and str(row['법정동코드'])[:2] in SUDO_PREFIX}
        return apt_df, region_map
    except Exception as e:
        st.error(f"❌ 데이터 로드 오류: {e}")
        st.stop()

apt_df, region_map = load_data()

# --- 3. 사이드바 (조건 입력) ---
st.sidebar.title("🏢 아파트 시세 조회")
st.sidebar.markdown("---")

region_options = {v: k for k, v in region_map.items()}
selected_region = st.sidebar.selectbox("1. 지역 선택 (서울/경기)", ["지역을 선택하세요"] + list(region_options.keys()))

if selected_region != "지역을 선택하세요":
    lawd_cd = region_options[selected_region]
    
    filtered_apts = apt_df[(apt_df['주소'].str.contains(selected_region, na=False)) & (apt_df['세대수'] >= 200)]
    sorted_apts = filtered_apts.sort_values(by='세대수', ascending=False).drop_duplicates(subset=['단지명_공시가격'])
    
    display_to_orig_map = {}
    for _, row in sorted_apts.iterrows():
        orig_name = row['단지명_공시가격']
        display_name = f"{orig_name} ({row['세대수']}세대, {row['연식']}년)"
        display_to_orig_map[display_name] = orig_name

    apt_display_names = list(display_to_orig_map.keys())
    
    if not apt_display_names:
        st.sidebar.warning("해당 지역에 200세대 이상 아파트가 없습니다.")
    else:
        selected_apt_display = st.sidebar.selectbox("2. 아파트 선택", ["아파트를 선택하세요"] + apt_display_names)
        
        if selected_apt_display != "아파트를 선택하세요":
            original_apt_name = display_to_orig_map[selected_apt_display]
            
            col1, col2 = st.sidebar.columns(2)
            with col1: start_date = st.date_input("시작월", datetime.date(2024, 1, 1))
            with col2: end_date = st.date_input("종료월", datetime.date(2024, 12, 31))
            
            start_ym, end_ym = start_date.strftime("%Y%m"), end_date.strftime("%Y%m")

            if end_date < start_date:
                st.sidebar.error("종료월이 시작월보다 앞설 수 없습니다.")
            elif st.sidebar.button("📊 시세 조회 시작", use_container_width=True):
                progress_bar = st.progress(0, text="데이터를 서버에서 가져오는 중...")

                cur_y, cur_m = int(start_ym[:4]), int(start_ym[4:])
                end_y, end_m = int(end_ym[:4]), int(end_ym[4:])
                total_months = (end_y - cur_y) * 12 + (end_m - cur_m) + 1

                results = []
                month_count = 0

                while True:
                    deal_ymd = f"{cur_y}{cur_m:02d}"
                    ym_format = f"{deal_ymd[:4]}-{deal_ymd[4:]}"
                    full_url = f"{API_URL}?serviceKey={API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}"

                    try:
                        time.sleep(0.1)
                        res = requests.get(full_url, timeout=10, verify=False)

                        if res.status_code == 200:
                            root = ET.fromstring(res.content)
                            header = root.find("header")

                            if header is not None and header.findtext("resultCode") == "000":
                                items = root.findall(".//item")
                                total_amt, count, prices = 0, 0, []

                                for item in items:
                                    cur_apt_name = (item.findtext('aptNm') or item.findtext('아파트') or "").strip()
                                    if cur_apt_name == original_apt_name.strip():
                                        price_str = item.findtext('dealAmount') or item.findtext('거래금액') or "0"
                                        price = int(price_str.replace(',', '').strip())
                                        if price > 0:
                                            total_amt += price; count += 1; prices.append(price)

                                if count > 0:
                                    prices.sort()
                                    results.append({"거래년월": ym_format, "평균가(만원)": round(total_amt / count), "최저가(만원)": prices[0], "최고가(만원)": prices[-1], "거래건수": count, "비고": "정상"})
                                else:
                                    results.append({"거래년월": ym_format, "평균가(만원)": 0, "최저가(만원)": 0, "최고가(만원)": 0, "거래건수": 0, "비고": "거래 없음"})
                            else:
                                msg = header.findtext("resultMsg") if header is not None else "응답 헤더 없음"
                                results.append({"거래년월": ym_format, "평균가(만원)": 0, "최저가(만원)": 0, "최고가(만원)": 0, "거래건수": -1, "비고": f"서버 응답 에러 ({msg})"})
                        else:
                            results.append({"거래년월": ym_format, "평균가(만원)": 0, "최저가(만원)": 0, "최고가(만원)": 0, "거래건수": -1, "비고": f"HTTP 에러 ({res.status_code})"})
                    except Exception as e:
                        results.append({"거래년월": ym_format, "평균가(만원)": 0, "최저가(만원)": 0, "최고가(만원)": 0, "거래건수": -1, "비고": f"통신 오류 ({e})"})

                    month_count += 1
                    progress_bar.progress(min(month_count / total_months, 1.0), text=f"{deal_ymd[:4]}년 {deal_ymd[4:]}월 조회 중...")

                    if cur_y == end_y and cur_m == end_m: break
                    cur_m += 1
                    if cur_m > 12: cur_m = 1; cur_y += 1

                progress_bar.empty()
                st.session_state["result_title"] = f"📈 [{selected_region}] {selected_apt_display} 시세 추이"
                st.session_state["result_data"] = results

            # --- 4. 결과 출력 (session_state에서 렌더링) ---
            if "result_data" in st.session_state and st.session_state["result_data"]:
                st.subheader(st.session_state["result_title"])
                res_df = pd.DataFrame(st.session_state["result_data"])
                chart_df = res_df[res_df["거래건수"] > 0].copy()
                if not chart_df.empty:
                    st.line_chart(chart_df.set_index("거래년월")[["평균가(만원)"]])

                def style_error(row): return ['color: #c5221f' if row['거래건수'] == -1 else ''] * len(row)
                st.dataframe(res_df.style.apply(style_error, axis=1).format({"평균가(만원)": "{:,}", "최저가(만원)": "{:,}", "최고가(만원)": "{:,}"}), use_container_width=True)
else:
    st.info("👈 왼쪽 사이드바에서 검색 조건을 설정해 주세요.")