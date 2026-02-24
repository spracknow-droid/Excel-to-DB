import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import re
import io

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 판매 데이터(계획/실적) SQLite DB 변환기")
st.info("💡 사용자가 업로드한 판매 데이터(계획/실적)를 통합하여 SQLite DB로 변환하는 페이지입니다.")

all_data = []

# [추가된 로직] 특정 컬럼 타입을 문자열로 고정하는 함수
def format_specific_columns(df):
    """'매출처' 등 특정 컬럼을 문자열 형식으로 변환"""
    target_col = '매출처'
    if target_col in df.columns:
        # nan 값을 빈 문자열로 처리하고 문자열로 변환
        df[target_col] = df[target_col].astype(str).replace(['nan', 'None', 'nan.0'], '')
        # 소수점(123.0)으로 표시되는 경우 제거
        df[target_col] = df[target_col].apply(lambda x: x.split('.')[0] if x.endswith('.0') else x)
    return df

# [공통 로직] 데이터 구분(Tagging) 함수
def add_data_tag(df):
    if df is None or df.empty:
        return df
    
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
        df = df.dropna(subset=['No'])
        df = df[df['No'].astype(str).str.strip() != ""]
        final_len = len(df)
        
        if initial_len > final_len:
            st.warning(f"⚠️ {filename}: 'No' 값이 없는 {initial_len - final_len}개의 행이 제외되었습니다.")
        return df.reset_index(drop=True)
    return df

# --- 사이드바: 3개 업로드 섹션 ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("로직 적용 및 데이터 병합 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 섹션
        for file in uploaded_plans:
            try:
                # 파일을 읽을 때 '매출처'가 있다면 문자열로 읽도록 시도 (없어도 에러 안 남)
                df = pd.read_excel(file, dtype={'매출처': str}) 
                df.columns = [str(c).strip() for c in df.columns]
                
                # 업로드 직후 타입 보정 로직 실행
                df = format_specific_columns(df)
                
                df = filter_invalid_rows(df, file.name)
                df = df.rename(columns={'품명': '품목명', '판매금액': '장부금액'})
                
                qty = pd.to_numeric(df.get('판매수량', 0), errors='coerce').fillna(0)
                price = pd.to_numeric(df.get('판매단가', 0), errors='coerce').fillna(0)
                df['판매금액'] = qty * price
                
                df = add_data_tag(df)
                all_data.append(df)
                st.write(f"✅ [계획섹션] {file.name} - 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 2] 판매실적 섹션
        for file in uploaded_results:
            try:
                df = pd.read_excel(file, dtype={'매출처': str})
                df.columns = [str(c).strip() for c in df.columns]
                
                # 업로드 직후 타입 보정 로직 실행
                df = format_specific_columns(df)
                
                df = filter_invalid_rows(df, file.name)
                df = add_data_tag(df)
                all_data.append(df)
                st.write(f"✅ [실적섹션] {file.name} - 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 3] DB 파일 (생략 - 기존 로직 유지)
        for file in uploaded_dbs:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(file.getvalue())
                tmp_path = tmp_file.name
            try:
                conn_old = sqlite3.connect(tmp_path)
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                for target_table in tables['name']:
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    # DB에서 가져온 데이터도 매출처 포맷팅 적용
                    df_db = format_specific_columns(df_db)
                    all_data.append(df_db)
                conn_old.close()
                st.write(f"✅ [DB] {file.name} - 데이터 로드 완료")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 병합 및 최종 정제 (기존 로직 유지)
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 컬럼명 정제 로직...
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

            # 문자열 컬럼 내 nan 처리
            cols_to_fix = combined_df.select_dtypes(include=['object']).columns
            for col in cols_to_fix:
                combined_df[col] = combined_df[col].astype(str).replace(['nan', 'None'], '')
            
            combined_df = combined_df.drop_duplicates()

            # [Step 5] DB 저장 및 다운로드 (기존 로직 유지)
            db_filename = "sales_integrated_final.db"
            if os.path.exists(db_filename): os.remove(db_filename)
            conn_new = sqlite3.connect(db_filename)
            combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace")
            conn_new.close()
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                combined_df.to_excel(writer, index=False, sheet_name='TotalData')
            excel_data = output.getvalue()

            status.update(label="모든 처리 완료!", state="complete", expanded=False)
            st.success(f"🎊 통합이 완료되었습니다! (총 행 수: **{len(combined_df):,}** 행)")
            st.dataframe(combined_df.head(10))
            
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("💾 통합 SQLite DB 다운로드", data=open(db_filename, "rb"), file_name=db_filename, use_container_width=True)
            with col2:
                st.download_button("Excel 통합파일 다운로드", data=excel_data, file_name="sales_integrated_final.xlsx", use_container_width=True)
