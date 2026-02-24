import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import re  # 특수문자 제거용

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 통합 데이터 변환기 (전표번호 기준 분류)")
st.markdown("'수익성계획전표번호' 기준 분류 및 SQL 호환성 강화 버전")

# --- 사이드바 및 함수 (기존과 동일) ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True, key="plan_uploader")
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True, key="result_uploader")
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True, key="db_uploader")

all_data = []

def process_classification(df):
    if df is None or df.empty:
        return
    
    # 컬럼명 앞뒤 공백 제거 (매우 중요)
    df.columns = [str(c).strip() for c in df.columns]
    
    if '수익성계획전표번호' in df.columns:
        is_plan = df['수익성계획전표번호'].notnull() & (df['수익성계획전표번호'].astype(str).str.strip() != "")
        
        df_plan = df[is_plan].copy()
        if not df_plan.empty:
            df_plan = df_plan.rename(columns={'품명': '품목명', '판매금액': '장부금액'})
            qty = pd.to_numeric(df_plan.get('판매수량', 0), errors='coerce').fillna(0)
            price = pd.to_numeric(df_plan.get('판매단가', 0), errors='coerce').fillna(0)
            df_plan['판매금액'] = qty * price
            df_plan['__데이터구분__'] = "판매계획"
            all_data.append(df_plan)
            
        df_result = df[~is_plan].copy()
        if not df_result.empty:
            df_result['__데이터구분__'] = "판매실적"
            all_data.append(df_result)
    else:
        df_copy = df.copy()
        df_copy['__데이터구분__'] = "판매실적"
        all_data.append(df_copy)

# --- 파일 로드 구간 (생략, 기존과 동일) ---
if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("데이터 통합 처리 중...", expanded=True) as status:
        # [기존과 동일한 파일 로드 루프 실행...]
        for file in uploaded_plans:
            try: process_classification(pd.read_excel(file))
            except Exception as e: st.error(f"Plan Error: {e}")
        for file in uploaded_results:
            try: process_classification(pd.read_excel(file))
            except Exception as e: st.error(f"Result Error: {e}")
        for file in uploaded_dbs:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(file.getvalue())
                tmp_path = tmp_file.name
            try:
                conn_old = sqlite3.connect(tmp_path)
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                for target_table in tables['name']:
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    process_classification(df_db)
                conn_old.close()
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 병합 및 정제
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 1. SQL 호환을 위한 컬럼명 정제 (핵심 수정 사항)
            # 특수문자 제거, 공백은 언더바로 변경
            clean_columns = []
            for col in combined_df.columns:
                clean_name = re.sub(r'\W+', '_', str(col)).strip('_') # 특수문자 -> _
                if not clean_name or clean_name[0].isdigit(): # 숫자로 시작하면 앞에 'col_' 붙임
                    clean_name = 'col_' + clean_name
                clean_columns.append(clean_name)
            combined_df.columns = clean_columns

            # 2. 타입 정제
            cols_to_fix = combined_df.select_dtypes(include=['object']).columns
            for col in cols_to_fix:
                combined_df[col] = combined_df[col].astype(str).replace(['nan', 'None'], '')

            combined_df = combined_df.drop_duplicates()

            # [Step 5] DB 파일 생성 (OperationalError 방지)
            db_filename = "integrated_sales_data.db"
            if os.path.exists(db_filename): os.remove(db_filename)
            
            try:
                conn_new = sqlite3.connect(db_filename)
                # chunksize를 추가하여 대량 데이터 처리 안정성 확보
                combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace", chunksize=1000)
                conn_new.close()
                status.update(label="통합 완료!", state="complete", expanded=False)
            except Exception as e:
                st.error(f"❌ DB 저장 중 치명적 오류: {e}")
                st.write("컬럼 목록을 확인하세요:", combined_df.columns.tolist())

            st.subheader("📊 통합 데이터 미리보기")
            st.dataframe(combined_df.head(10))
            with open(db_filename, "rb") as f:
                st.download_button("💾 통합 DB 다운로드", f, file_name=db_filename)
else:
    st.info("파일을 업로드해 주세요.")
