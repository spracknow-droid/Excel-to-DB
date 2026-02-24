import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import re

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 통합 데이터 변환기 (업로드 섹션 기준 컬럼 수정)")
st.info("💡 컬럼명 변경 및 수식 계산은 오직 '1️⃣ 판매계획' 섹션에 업로드된 파일에만 적용됩니다.")

all_data = []

# [공통 로직] 데이터 구분(Tagging) 함수
def add_data_tag(df):
    if df is None or df.empty:
        return df
    
    # 1. 전표번호 유무에 따른 데이터 구분 (사용자 지시사항)
    if '수익성계획전표번호' in df.columns:
        is_plan = df['수익성계획전표번호'].notnull() & (df['수익성계획전표번호'].astype(str).str.strip() != "")
        df.loc[is_plan, '__데이터구분__'] = "판매계획"
        df.loc[~is_plan, '__데이터구분__'] = "판매실적"
    else:
        df['__데이터구분__'] = "판매실적"
    return df

# [공통 로직] 'No' 컬럼 기반 유효 데이터 필터링 함수
def filter_invalid_rows(df, filename):
    if 'No' in df.columns:
        initial_len = len(df)
        # 'No' 컬럼이 NaN(비어있음)이거나 공백인 행 제거
        df = df.dropna(subset=['No'])
        df = df[df['No'].astype(str).str.strip() != ""]
        final_len = len(df)
        
        if initial_len > final_len:
            st.warning(f"⚠️ {filename}: 'No' 값이 없는 {initial_len - final_len}개의 행이 제외되었습니다.")
    return df.reset_index(drop=True)

# --- 사이드바: 3개 업로드 섹션 ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("로직 적용 및 데이터 병합 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 섹션 (컬럼 수정 로직 적용)
        for file in uploaded_plans:
            try:
                df = pd.read_excel(file)
                df.columns = [str(c).strip() for c in df.columns] # 공백 제거
                
                # 'No' 컬럼값 없는 행 삭제 로직 추가
                df = filter_invalid_rows(df, file.name)
                
                # 1. 컬럼명 변경 (품명 -> 품목명, 판매금액 -> 장부금액)
                df = df.rename(columns={'품명': '품목명', '판매금액': '장부금액'})
                
                # 2. 신규 판매금액 생성 (판매수량 * 판매단가)
                qty = pd.to_numeric(df.get('판매수량', 0), errors='coerce').fillna(0)
                price = pd.to_numeric(df.get('판매단가', 0), errors='coerce').fillna(0)
                df['판매금액'] = qty * price
                
                # 3. 데이터 구분 태그 추가 (전표번호 기준)
                df = add_data_tag(df)
                
                all_data.append(df)
                st.write(f"✅ [계획섹션] {file.name} - 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 2] 판매실적 섹션 (원본 컬럼 유지)
        for file in uploaded_results:
            try:
                df = pd.read_excel(file)
                df.columns = [str(c).strip() for c in df.columns]
                
                # 'No' 컬럼값 없는 행 삭제 로직 추가
                df = filter_invalid_rows(df, file.name)
                
                # 컬럼 수정 없이 태그만 추가
                df = add_data_tag(df)
                
                all_data.append(df)
                st.write(f"✅ [실적섹션] {file.name} - 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 3] DB 파일 (원본 완전 보존)
        for file in uploaded_dbs:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(file.getvalue())
                tmp_path = tmp_file.name
            try:
                conn_old = sqlite3.connect(tmp_path)
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                for target_table in tables['name']:
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    all_data.append(df_db)
                conn_old.close()
                st.write(f"✅ [DB] {file.name} - 데이터 로드 완료")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 병합 및 최종 정제
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # SQL/Streamlit 호환을 위한 컬럼 중복 해결 로직
            new_cols = []
            col_counts = {}
            for col in combined_df.columns:
                clean_name = re.sub(r'\W+', '_', str(col)).strip('_')
                if clean_name in col_counts:
                    col_counts[clean_name] += 1
                    final_name = f"{clean_name}_{col_counts[clean_name]}"
                else:
                    col_counts[clean_name] = 0
                    final_name = clean_name
                new_cols.append(final_name)
            combined_df.columns = new_cols

            # 데이터 타입 통일 및 중복 제거
            cols_to_fix = combined_df.select_dtypes(include=['object']).columns
            for col in cols_to_fix:
                combined_df[col] = combined_df[col].astype(str).replace(['nan', 'None'], '')
            combined_df = combined_df.drop_duplicates()

            # [Step 5] 통합 DB 생성
            db_filename = "sales_integrated_final.db"
            if os.path.exists(db_filename): os.remove(db_filename)
            conn_new = sqlite3.connect(db_filename)
            combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace")
            conn_new.close()
            
            status.update(label="모든 처리 완료!", state="complete", expanded=False)
            
            st.subheader("📊 통합 데이터 미리보기")
            st.dataframe(combined_df.head(10))
            
            with open(db_filename, "rb") as f:
                st.download_button("💾 통합 DB 다운로드", f, file_name=db_filename)
