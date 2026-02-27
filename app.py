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

        # 문자열 공백 제거
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        if "SLSSPN" in fname:
            target_table = "plan_data"
        elif "BILBIV" in fname:
            target_table = "actual_data"
            if '매출번호' in df.columns:
                df = df[df['매출번호'].astype(str).str.contains('합계') == False]
        else:
            continue

        try:
            # 테이블 존재 여부 확인
            existing_df = pd.read_sql(f"SELECT * FROM {target_table} LIMIT 1", conn)

            # 컬럼 구조 맞추기
            existing_columns = pd.read_sql(f"SELECT * FROM {target_table} LIMIT 0", conn).columns

            # 누락 컬럼 추가
            for col in existing_columns:
                if col not in df.columns:
                    df[col] = None

            # 컬럼 순서 정렬
            df = df[existing_columns]

            # append 방식으로 추가
            df.to_sql(target_table, conn, if_exists="append", index=False)

        except Exception:
            # 테이블이 없으면 신규 생성
            df.drop_duplicates().to_sql(target_table, conn, if_exists="replace", index=False)

        # --- 완전 중복 제거 (SQL 기반) ---
        columns = df.columns.tolist()
        group_cols = ", ".join(columns)

        conn.execute(f"""
            DELETE FROM {target_table}
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM {target_table}
                GROUP BY {group_cols}
            )
        """)
        conn.commit()

        st.success(f"✅ {fname} 누적 완료")

# --- 데이터 확인 ---
st.divider()
tab1, tab2 = st.tabs(["판매계획 (Plan)", "매출리스트 (Actual)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM plan_data", conn)
        if not df_p.empty:
            st.write(f"현재 누적 데이터: **{len(df_p)}** 행")
            st.dataframe(df_p, use_container_width=True)
        else:
            st.info("데이터가 비어있습니다.")
    except:
        st.info("업로드된 판매계획 데이터가 없습니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM actual_data", conn)
        if not df_a.empty:
            st.write(f"현재 누적 데이터: **{len(df_a)}** 행")
            st.dataframe(df_a, use_container_width=True)
        else:
            st.info("데이터가 비어있습니다.")
    except:
        st.info("업로드된 매출리스트 데이터가 없습니다.")

# --- 내보내기 ---
st.divider()
col1, col2 = st.columns(2)

with col1:
    temp_db_path = "export.db"
    with sqlite3.connect(temp_db_path) as export_conn:
        st.session_state.db_conn.backup(export_conn)
    with open(temp_db_path, "rb") as f:
        st.download_button("💾 SQLite DB 다운로드", f, "integrated_data.db")
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)

with col2:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        try:
            pd.read_sql("SELECT * FROM plan_data", conn).to_excel(writer, sheet_name='Plan', index=False)
        except:
            pass
        try:
            pd.read_sql("SELECT * FROM actual_data", conn).to_excel(writer, sheet_name='Actual', index=False)
        except:
            pass
    st.download_button("📊 Excel 통합 파일 다운로드", output.getvalue(), "integrated_data.xlsx")
