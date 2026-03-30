import os
import streamlit as st
import pandas as pd
import requests
import urllib3
import xml.etree.ElementTree as ET
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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
                # 월 목록 생성
                months = []
                cur_y, cur_m = int(start_ym[:4]), int(start_ym[4:])
                end_y, end_m = int(end_ym[:4]), int(end_ym[4:])
                while True:
                    months.append(f"{cur_y}{cur_m:02d}")
                    if cur_y == end_y and cur_m == end_m:
                        break
                    cur_m += 1
                    if cur_m > 12:
                        cur_m = 1; cur_y += 1

                progress_bar = st.progress(0, text=f"총 {len(months)}개월 병렬 조회 중...")

                # 단일 월 캐시 fetch (1시간 TTL)
                @st.cache_data(ttl=3600, show_spinner=False)
                def _fetch_month_price(lcd, apt, ym):
                    url = f"{API_URL}?serviceKey={API_KEY}&pageNo=1&numOfRows=1000&LAWD_CD={lcd}&DEAL_YMD={ym}"
                    try:
                        res = requests.get(url, timeout=10, verify=False)
                        if res.status_code != 200:
                            return {"ok": False, "msg": f"HTTP {res.status_code}", "prices": []}
                        root = ET.fromstring(res.content)
                        header = root.find("header")
                        if header is None or header.findtext("resultCode") != "000":
                            msg = (header.findtext("resultMsg") if header is not None else "헤더 없음")
                            return {"ok": False, "msg": msg, "prices": []}
                        prices = []
                        for item in root.findall(".//item"):
                            name = (item.findtext("aptNm") or "").strip()
                            if name == apt.strip():
                                try:
                                    p = int((item.findtext("dealAmount") or "0").replace(",", "").strip())
                                    if p > 0:
                                        prices.append(p)
                                except ValueError:
                                    pass
                        return {"ok": True, "msg": "정상", "prices": prices}
                    except Exception as e:
                        return {"ok": False, "msg": str(e), "prices": []}

                # 병렬 호출
                raw = {}
                with ThreadPoolExecutor(max_workers=8) as executor:
                    future_to_ym = {
                        executor.submit(_fetch_month_price, lawd_cd, original_apt_name, ym): ym
                        for ym in months
                    }
                    done = 0
                    for future in as_completed(future_to_ym):
                        ym = future_to_ym[future]
                        raw[ym] = future.result()
                        done += 1
                        progress_bar.progress(done / len(months), text=f"{done}/{len(months)}개월 완료...")

                # 결과 정리 (월 순서 유지)
                results = []
                for ym in months:
                    ym_format = f"{ym[:4]}-{ym[4:]}"
                    data = raw[ym]
                    if not data["ok"]:
                        results.append({"거래년월": ym_format, "평균가(만원)": 0, "최저가(만원)": 0, "최고가(만원)": 0, "거래건수": -1, "비고": data["msg"]})
                    elif data["prices"]:
                        ps = sorted(data["prices"])
                        results.append({"거래년월": ym_format, "평균가(만원)": round(sum(ps) / len(ps)), "최저가(만원)": ps[0], "최고가(만원)": ps[-1], "거래건수": len(ps), "비고": "정상"})
                    else:
                        results.append({"거래년월": ym_format, "평균가(만원)": 0, "최저가(만원)": 0, "최고가(만원)": 0, "거래건수": 0, "비고": "거래 없음"})

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