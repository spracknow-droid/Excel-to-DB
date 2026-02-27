import streamlit as st
import pandas as pd
import sqlite3
import os

# DB 파일명 설정 (기본값)
DB_NAME = "integrated_sales.db"

def get_connection(db_path):
    return sqlite3.connect(db_path)

st.set_page_config(page_title="데이터 입고 시스템", layout="wide")
st.title("🗄️ 판매 데이터 통합 및 DB 저장 도구")

# --- 사이드바: 3가지 업로드 소스 ---
with st.sidebar:
    st.header("📂 데이터 소스 업로드")
    
    # 소스 1: 기존 SQLite DB 파일 (가장 먼저 처리)
    st.subheader("1. 기존 DB 파일")
    uploaded_db = st.file_uploader("기존 .db 또는 .sqlite 파일", type=["db", "sqlite"])
    if uploaded_db:
        with open(DB_NAME, "wb") as f:
            f.write(uploaded_db.getbuffer())
        st.success("기존 DB 로드 완료")

    st.divider()

    # 소스 2 & 3: 엑셀 파일들 (판매계획 vs 매출리스트)
    st.subheader("2 & 3. 신규 엑셀 데이터")
    excel_files = st.file_uploader(
        "판매계획 또는 매출리스트 (다중 선택)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )

# --- 메인 로직: 엑셀 데이터를 DB로 이동 ---
if excel_files:
    conn = get_connection(DB_NAME)
    
    for file in excel_files:
        df = pd.read_excel(file)
        fname = file.name
        
        # 파일명 기반 자동 테이블 분류
        if "계획" in fname:
            target_table = "plan_data"
            color = "blue"
        elif "매출" in fname:
            target_table = "actual_data"
            color = "green"
        else:
            st.error(f"❌ '{fname}': 파일명에 '계획' 또는 '매출' 키워드가 없습니다.")
            continue
            
        # 데이터 누적 저장 (Append)
        try:
            df.to_sql(target_table, conn, if_exists="append", index=False)
            st.write(f":{color}[**{fname}**] -> `{target_table}` 테이블에 저장 성공")
        except Exception as e:
            st.error(f"저장 오류 ({fname}): {e}")
    
    conn.close()

# --- DB 데이터 확인 (View Only) ---
st.divider()
st.header("📋 현재 DB 저장 현황 (Raw Data)")

if os.path.exists(DB_NAME):
    conn = get_connection(DB_NAME)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📝 판매계획 (plan_data)")
        try:
            plan_df = pd.read_sql("SELECT * FROM plan_data", conn)
            st.caption(f"총 {len(plan_df)}행")
            st.dataframe(plan_df, height=400)
        except:
            st.info("판매계획 데이터가 없습니다.")

    with col2:
        st.subheader("💰 매출리스트 (actual_data)")
        try:
            actual_df = pd.read_sql("SELECT * FROM actual_data", conn)
            st.caption(f"총 {len(actual_df)}행")
            st.dataframe(actual_df, height=400)
        except:
            st.info("매출리스트 데이터가 없습니다.")
            
    conn.close()
else:
    st.warning("생성된 데이터베이스 파일이 없습니다. 파일을 업로드하여 시작하세요.")
