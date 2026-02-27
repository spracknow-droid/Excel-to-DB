import streamlit as st
import pandas as pd
import sqlite3

# --- 💡 핵심 변경: 메모리 내 DB 사용 ---
# 세션 동안만 유지되도록 streamlit의 session_state에 연결을 저장합니다.
if 'db_conn' not in st.session_state:
    # ':memory:'는 파일을 생성하지 않고 RAM에만 데이터를 저장합니다.
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)
    # 초기 테이블 생성
    conn = st.session_state.db_conn
    conn.execute("CREATE TABLE IF NOT EXISTS plan_data (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    conn.execute("CREATE TABLE IF NOT EXISTS actual_data (id INTEGER PRIMARY KEY AUTOINCREMENT)")

conn = st.session_state.db_conn

st.set_page_config(page_title="휘발성 데이터 통합 도구", layout="wide")
st.title("🔋 세션 기반 실시간 데이터 통합 (휘발성)")
st.warning("⚠️ 주의: 이 앱은 메모리 상에서만 작동하므로, 브라우저 새로고침 시 모든 데이터가 즉시 삭제됩니다.")

# --- 사이드바 ---
with st.sidebar:
    st.header("📂 데이터 업로드")
    excel_files = st.file_uploader(
        "시스템 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )

# --- 메인 로직 ---
if excel_files:
    for file in excel_files:
        df = pd.read_excel(file)
        fname = file.name
        
        if "SLSSPN" in fname:
            target_table = "plan_data"
        elif "BILBIV" in fname:
            target_table = "actual_data"
            # 합계 행 삭제
            if '매출번호' in df.columns:
                df = df[df['매출번호'].astype(str).str.contains('합계') == False]
        else:
            continue
            
        # 메모리 DB에 저장
        df.to_sql(target_table, conn, if_exists="append", index=False)
        st.success(f"✅ {fname} 임시 저장됨")

# --- 데이터 확인 ---
st.divider()
st.header("📋 세션 내 데이터 확인")

tab1, tab2 = st.tabs(["판매계획", "매출리스트"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM plan_data", conn)
        if not df_p.empty: st.dataframe(df_p, use_container_width=True)
        else: st.info("데이터가 없습니다.")
    except: st.info("데이터가 없습니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM actual_data", conn)
        if not df_a.empty: st.dataframe(df_a, use_container_width=True)
        else: st.info("데이터가 없습니다.")
    except: st.info("데이터가 없습니다.")
