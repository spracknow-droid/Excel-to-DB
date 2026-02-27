import streamlit as st
import pandas as pd
import sqlite3

# DB 파일명 설정
DB_NAME = "sales_archive.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

st.set_page_config(page_title="데이터 입고 시스템", layout="centered")
st.title("🗄️ 판매 데이터 DB 저장 도구")
st.info("시각화 없이 '판매계획'과 '매출리스트'를 DB로 통합하는 데 집중합니다.")

# --- 사이드바: 파일 관리 ---
with st.sidebar:
    st.header("1. 데이터 소스")
    # 기존 DB가 있다면 업로드하여 교체 가능
    uploaded_db = st.file_uploader("기존 DB 파일 불러오기", type="db")
    if uploaded_db:
        with open(DB_NAME, "wb") as f:
            f.write(uploaded_db.getbuffer())
        st.success("기존 DB를 로드했습니다.")

    # 엑셀 파일들 업로드
    uploaded_files = st.file_uploader(
        "신규 엑셀 파일 업로드 (다중 선택 가능)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )

# --- 메인 로직: 저장 기능 ---
if uploaded_files:
    conn = get_connection()
    
    for file in uploaded_files:
        df = pd.read_excel(file)
        fname = file.name
        
        # 파일명에 따른 테이블 자동 분류
        if "계획" in fname:
            target_table = "plan_data"
        elif "매출" in fname:
            target_table = "actual_data"
        else:
            st.error(f"⚠️ '{fname}'은(는) 분류를 알 수 없어 제외되었습니다. (파일명 확인 필요)")
            continue
            
        # DB 저장 (기존 데이터 아래에 추가)
        df.to_sql(target_table, conn, if_exists="append", index=False)
        st.success(f"✅ {fname} -> {target_table} 테이블에 저장 완료")
    
    conn.close()

# --- 저장 결과 확인 (단순 리스트 출력) ---
st.divider()
st.header("📋 현재 DB 저장 현황")

tab1, tab2 = st.tabs(["판매계획 테이블", "매출리스트 테이블"])

with tab1:
    try:
        conn = get_connection()
        plan_view = pd.read_sql("SELECT * FROM plan_data", conn)
        st.write(f"총 레코드 수: {len(plan_view)}건")
        st.dataframe(plan_view, use_container_width=True)
        conn.close()
    except:
        st.write("아직 데이터가 없습니다.")

with tab2:
    try:
        conn = get_connection()
        actual_view = pd.read_sql("SELECT * FROM actual_data", conn)
        st.write(f"총 레코드 수: {len(actual_view)}건")
        st.dataframe(actual_view, use_container_width=True)
        conn.close()
    except:
        st.write("아직 데이터가 없습니다.")
