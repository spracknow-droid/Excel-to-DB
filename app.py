import streamlit as st
import pandas as pd
import sqlite3
import os

# DB 설정
DB_NAME = "sales_archive.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

st.set_page_config(page_title="Data Ingestion System", layout="wide")
st.title("🗄️ 시스템 파일 자동 분류 및 DB 통합")

# --- 사이드바: 3-Source 업로드 ---
with st.sidebar:
    st.header("📂 데이터 소스")
    
    # 1. 기존 DB 로드
    uploaded_db = st.file_uploader("기존 SQLite DB (.db)", type=["db", "sqlite"])
    if uploaded_db:
        with open(DB_NAME, "wb") as f:
            f.write(uploaded_db.getbuffer())
        st.success("기존 데이터베이스 연결됨")

    st.divider()

    # 2 & 3. 시스템 엑셀 파일 (다중 업로드)
    st.subheader("엑셀 파일 (판매계획/매출리스트)")
    excel_files = st.file_uploader(
        "시스템 다운로드 파일을 그대로 올리세요", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )

# --- 메인 로직: 파일명 규칙 기반 분류 및 저장 ---
if excel_files:
    conn = get_connection()
    
    for file in excel_files:
        df = pd.read_excel(file)
        fname = file.name
        
        # 💡 사용자가 알려준 파일명 규칙 적용
        if "SLSSPN" in fname:
            target_table = "plan_data"
            label = "📝 판매계획 (SLSSPN)"
        elif "BILBIV" in fname:
            target_table = "actual_data"
            label = "💰 매출리스트 (BILBIV)"
        else:
            st.error(f"❌ 분류 불가: '{fname}' (파일명 규칙에 맞지 않음)")
            continue
            
        # DB 저장 (누적)
        df.to_sql(target_table, conn, if_exists="append", index=False)
        st.toast(f"{fname} 저장 완료!", icon="✅")
        st.write(f"**{label}** 저장됨: `{fname}` ({len(df)}건)")
    
    conn.close()

# --- 데이터 확인용 뷰어 ---
st.divider()
st.header("📋 데이터 테이블 미리보기")

if os.path.exists(DB_NAME):
    conn = get_connection()
    tab1, tab2 = st.tabs(["판매계획 (Plan)", "매출리스트 (Actual)"])
    
    with tab1:
        try:
            df_p = pd.read_sql("SELECT * FROM plan_data", conn)
            st.dataframe(df_p, use_container_width=True)
        except:
            st.info("판매계획 데이터가 없습니다.")
            
    with tab2:
        try:
            df_a = pd.read_sql("SELECT * FROM actual_data", conn)
            st.dataframe(df_a, use_container_width=True)
        except:
            st.info("매출리스트 데이터가 없습니다.")
    conn.close()
