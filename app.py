import streamlit as st
import pandas as pd
import sqlite3
import os
import io

# --- 💡 핵심 변경: 메모리 내 DB 사용 ---
if 'db_conn' not in st.session_state:
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)
    conn = st.session_state.db_conn
    conn.execute("CREATE TABLE IF NOT EXISTS plan_data (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    conn.execute("CREATE TABLE IF NOT EXISTS actual_data (id INTEGER PRIMARY KEY AUTOINCREMENT)")

conn = st.session_state.db_conn

st.set_page_config(page_title="데이터 통합 도구", layout="wide")
st.title("🔋 세션 기반 실시간 데이터 통합")

# --- 사이드바 ---
with st.sidebar:
    st.header("📂 데이터 업로드")
    
    # 1. 엑셀 파일 업로드
    excel_files = st.file_uploader(
        "1️⃣ 시스템 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )
    
    st.divider()
    
    # 2. SQLite DB 파일 업로드
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])

# --- 메인 로직: DB 업로드 처리 ---
if uploaded_db:
    with open("temp_uploaded.db", "wb") as f:
        f.write(uploaded_db.getbuffer())
    with sqlite3.connect("temp_uploaded.db") as temp_conn:
        temp_conn.backup(st.session_state.db_conn)
    os.remove("temp_uploaded.db")
    st.sidebar.success("✅ DB 파일 로드 완료")

# --- 메인 로직: 엑셀 파일 처리 ---
if excel_files:
    for file in excel_files:
        df = pd.read_excel(file)
        fname = file.name
        
        if "SLSSPN" in fname:
            target_table = "plan_data"
        elif "BILBIV" in fname:
            target_table = "actual_data"
            if '매출번호' in df.columns:
                df = df[df['매출번호'].astype(str).str.contains('합계') == False]
        else:
            continue
            
        df.to_sql(target_table, conn, if_exists="append", index=False)
        st.success(f"✅ {fname} 임시 저장됨")

# --- 데이터 확인 ---
st.divider()
st.header("📋 세션 내 데이터 확인")

tab1, tab2 = st.tabs(["판매계획 (Plan)", "매출리스트 (Actual)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM plan_data", conn)
        if not df_p.empty: st.dataframe(df_p, use_container_width=True)
        else: st.info("판매계획 데이터가 없습니다.")
    except: st.info("테이블 생성 전입니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM actual_data", conn)
        if not df_a.empty: st.dataframe(df_a, use_container_width=True)
        else: st.info("매출리스트 데이터가 없습니다.")
    except: st.info("테이블 생성 전입니다.")

# --- 데이터 내보내기 ---
st.divider()
st.header("📥 데이터 내보내기")

col1, col2 = st.columns(2)

with col1:
    if st.button("SQLite DB 파일 준비"):
        temp_db_path = "export_session_data.db"
        with sqlite3.connect(temp_db_path) as export_conn:
            st.session_state.db_conn.backup(export_conn)
        
        with open(temp_db_path, "rb") as f:
            st.download_button(
                label="💾 DB 다운로드",
                data=f.read(),
                file_name="integrated_data.db",
                mime="application/x-sqlite3"
            )
        if os.path.exists(temp_db_path): os.remove(temp_db_path)

with col2:
    if st.button("Excel 통합 파일 준비"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # DB에서 데이터를 다시 읽어와 엑셀 시트로 저장
            pd.read_sql("SELECT * FROM plan_data", conn).to_excel(writer, sheet_name='Plan_Data', index=False)
            pd.read_sql("SELECT * FROM actual_data", conn).to_excel(writer, sheet_name='Actual_Data', index=False)
        
        st.download_button(
            label="📊 Excel 다운로드",
            data=output.getvalue(),
            file_name="integrated_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
