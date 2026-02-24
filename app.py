import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import re

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 통합 데이터 변환기 (컬럼 중복 해결)")

# --- 사이드바 및 함수 (기존 로직 유지) ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True, key="plan_uploader")
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True, key="result_uploader")
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True, key="db_uploader")

all_data = []

def process_classification(df):
    if df is None or df.empty:
        return
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

# --- 파일 로드 (이전과 동일) ---
if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("데이터 처리 중...", expanded=True) as status:
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

        # [Step 4] 병합 및 중복 컬럼명 해결 (ValueError 방지 핵심)
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 1. SQL 호환성 및 중복 컬럼명 처리 로직
            new_cols = []
            col_counts = {}
            for col in combined_df.columns:
                # 특수문자 정제
                clean_name = re.sub(r'\W+', '_', str(col)).strip('_')
                if not clean_name or clean_name[0].isdigit():
                    clean_name = 'col_' + clean_name
                
                # 중복 이름 처리 (중복 시 이름_1, 이름_2 형식)
                if clean_name in col_counts:
                    col_counts[clean_name] += 1
                    final_name = f"{clean_name}_{col_counts[clean_name]}"
                else:
                    col_counts[clean_name] = 0
                    final_name = clean_name
                new_cols.append(final_name)
            
            combined_df.columns = new_cols

            # 2. 타입 정제
            cols_to_fix = combined_df.select_dtypes(include=['object']).columns
            for col in cols_to_fix:
                combined_df[col] = combined_df[col].astype(str).replace(['nan', 'None'], '')

            combined_df = combined_df.drop_duplicates()

            # [Step 5] DB 저장 및 출력
            db_filename = "final_integrated_data.db"
            if os.path.exists(db_filename): os.remove(db_filename)
            conn_new = sqlite3.connect(db_filename)
            combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace")
            conn_new.close()
            
            status.update(label="통합 완료!", state="complete", expanded=False)
            st.subheader("📊 통합 데이터 미리보기")
            st.dataframe(combined_df.head(10)) # 이제 에러 없이 표시됨
            
            with open(db_filename, "rb") as f:
                st.download_button("💾 통합 DB 다운로드", f, file_name=db_filename)
else:
    st.info("파일을 업로드해 주세요.")
