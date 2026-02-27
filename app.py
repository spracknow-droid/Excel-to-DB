import streamlit as st
import pandas as pd
import sqlite3
import os
import io

# --- 💡 세션 기반 메모리 DB 초기화 ---
if 'db_conn' not in st.session_state:
    # 단순하게 연결만 생성 (테이블은 데이터 업로드 시 자동 생성됨)
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)

conn = st.session_state.db_conn

st.set_page_config(page_title="데이터 통합 도구", layout="wide")
st.title("🔋 세션 기반 실시간 데이터 통합")

# --- 사이드바: 업로드 공간 분리 ---
with st.sidebar:
    st.header("📂 데이터 업로드")
    
    excel_files = st.file_uploader(
        "1️⃣ 시스템 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )
    
    st.divider()
    
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])

# --- 로직 1: 업로드된 DB 파일 처리 ---
if uploaded_db:
    with open("temp_uploaded.db", "wb") as f:
        f.write(uploaded_db.getbuffer())
    with sqlite3.connect("temp_uploaded.db") as temp_conn:
        temp_conn.backup(st.session_state.db_conn)
    os.remove("temp_uploaded.db")
    st.sidebar.success("✅ DB 파일 로드 완료")

# --- 로직 2: 엑셀 파일 처리 ---
if excel_files:
    for file in excel_files:
        df = pd.read_excel(file)
        fname = file.name
        
        # 파일 내 자체 중복 제거
        df = df.drop_duplicates()
        
        if "SLSSPN" in fname:
            target_table = "plan_data"
        elif "BILBIV" in fname:
            target_table = "actual_data"
            if '매출번호' in df.columns:
                df = df[df['매출번호'].astype(str).str.contains('합계') == False]
        else:
            continue
            
        try:
            # 기존 데이터가 있으면 불러와서 병합 후 중복 제거
            existing_df = pd.read_sql(f"SELECT * FROM {target_table}", conn)
            combined_df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates()
            combined_df.to_sql(target_table, conn, if_exists="replace", index=False)
            st.success(f"✅ {fname} 통합 완료 (중복 제거됨)")
        except:
            # 테이블이 없으면 새로 생성 (id, dummy 없음)
            df.to_sql(target_table, conn, if_exists="replace", index=False)
            st.success(f"✅ {fname} 신규 저장됨")

# --- 데이터 확인 (Tabs) ---
st.divider()
st.header("📋 세션 내 데이터 확인")

tab1, tab2 = st.tabs(["판매계획 (Plan)", "매출리스트 (Actual)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM plan_data", conn)
        if not df_p.empty: 
            st.write(f"총 행 수: {len(df_p)}")
            st.dataframe(df_p, use_container_width=True)
        else: st.info("판매계획 데이터가 없습니다.")
    except: st.info("데이터를 업로드해주세요.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM actual_data", conn)
        if not df_a.empty: 
            st.write(f"총 행 수: {len(df_a)}")
            st.dataframe(df_a, use_container_width=True)
        else: st.info("매출리스트 데이터가 없습니다.")
    except: st.info("데이터를 업로드해주세요.")

# --- 데이터 내보내기 ---
st.divider()
st.header("📥 데이터 내보내기")

col1, col2 = st.columns(2)

with col1:
    if st.button("SQLite DB 파일 생성"):
        temp_db_path = "export_session_data.db"
        with sqlite3.connect(temp_db_path) as export_conn:
            st.session_state.db_conn.backup(export_conn)
        
        with open(temp_db_path, "rb") as f:
            st.download_button(
                label="💾 DB 파일 다운로드",
                data=f.read(),
                file_name="integrated_data.db",
                mime="application/x-sqlite3"
            )
        if os.path.exists(temp_db_path): os.remove(temp_db_path)

with col2:
    if st.button("Excel 통합 파일 생성"):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            try:
                pd.read_sql("SELECT * FROM plan_data", conn).to_excel(writer, sheet_name='Plan_Data', index=False)
            except: pass
            try:
                pd.read_sql("SELECT * FROM actual_data", conn).to_excel(writer, sheet_name='Actual_Data', index=False)
            except: pass
        
        st.download_button(
            label="📊 Excel 파일 다운로드",
            data=output.getvalue(),
            file_name="integrated_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
