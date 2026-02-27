import streamlit as st
import pandas as pd
import sqlite3
import os
import io

# --- 💡 세션 기반 메모리 DB 초기화 ---
if 'db_conn' not in st.session_state:
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)

conn = st.session_state.db_conn

st.set_page_config(page_title="데이터 통합 도구", layout="wide")
st.title("🔋 세션 기반 실시간 데이터 통합")

# --- 사이드바 ---
with st.sidebar:
    st.header("📂 데이터 업로드")
    excel_files = st.file_uploader(
        "1️⃣ 시스템 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )
    st.divider()
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])
    
    # [추가] 초기화 버튼 (데이터가 꼬였을 때를 대비)
    if st.sidebar.button("🗑 전체 데이터 초기화"):
        st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)
        st.rerun()

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
        
        # 전처리: 공백 제거
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        if "SLSSPN" in fname:
            target_table = "plan_data"
            # 판매계획(SLSSPN)은 파일 전체가 최신본인 경우가 많으므로 
            # 단순히 합치지 않고, 행 전체가 중복인 것만 제거하거나 
            # 필요 시 'if_exists="replace"'를 고민해야 함. 
            # 여기서는 요청대로 '중복 행 삭제'를 행 전체 기준으로 엄격하게 적용.
            df = df.drop_duplicates()
            
        elif "BILBIV" in fname:
            target_table = "actual_data"
            if '매출번호' in df.columns:
                df = df[df['매출번호'].astype(str).str.contains('합계') == False]
                df = df.drop_duplicates(subset=['매출번호'], keep='last')
        else:
            continue

        try:
            # 기존 데이터 읽기
            existing_df = pd.read_sql(f"SELECT * FROM {target_table}", conn)
            
            # 기존 데이터와 새 데이터를 합친 후 전체 중복 제거
            # keep='last'를 통해 새로 올린 파일의 데이터를 우선시함
            combined_df = pd.concat([existing_df, df], ignore_index=True).drop_duplicates(keep='last')
            
            combined_df.to_sql(target_table, conn, if_exists="replace", index=False)
            st.success(f"✅ {fname} 반영 완료")
        except:
            # 테이블이 없으면 생성
            df.to_sql(target_table, conn, if_exists="replace", index=False)
            st.success(f"✅ {fname} 신규 저장")

# --- 데이터 확인 ---
st.divider()
tab1, tab2 = st.tabs(["판매계획 (Plan)", "매출리스트 (Actual)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM plan_data", conn)
        st.write(f"데이터 수: {len(df_p)}")
        st.dataframe(df_p, use_container_width=True)
    except: st.info("데이터가 없습니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM actual_data", conn)
        st.write(f"데이터 수: {len(df_a)}")
        st.dataframe(df_a, use_container_width=True)
    except: st.info("데이터가 없습니다.")

# --- 내보내기 ---
st.divider()
col1, col2 = st.columns(2)

with col1:
    temp_db_path = "export.db"
    with sqlite3.connect(temp_db_path) as export_conn:
        st.session_state.db_conn.backup(export_conn)
    with open(temp_db_path, "rb") as f:
        st.download_button("💾 SQLite DB 다운로드", f, "integrated_data.db")
    if os.path.exists(temp_db_path): os.remove(temp_db_path)

with col2:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        try: pd.read_sql("SELECT * FROM plan_data", conn).to_excel(writer, "Plan", index=False)
        except: pass
        try: pd.read_sql("SELECT * FROM actual_data", conn).to_excel(writer, "Actual", index=False)
        except: pass
    st.download_button("📊 Excel 통합 파일 다운로드", output.getvalue(), "integrated_data.xlsx")
