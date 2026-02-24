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

# [함수] 특정 컬럼 타입을 문자열로 고정 및 데이터 클리닝
def format_specific_columns(df):
    """'매출처' 등 코드 성격의 컬럼을 깨끗한 문자열 형식으로 변환"""
    target_cols = ['매출처', '품목명', '품번'] 
    for col in target_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'nan.0'], '')
            df[col] = df[col].apply(lambda x: x.split('.')[0] if x.endswith('.0') else x)
            df[col] = df[col].str.strip()
    return df

# [공통 로직] 데이터 구분(Tagging) 함수
def add_data_tag(df):
    if df is None or df.empty:
        return df
    
    # 이미 데이터구분 컬럼이 있는 경우 중복 생성을 방지
    tag_col = '__데이터구분__'
    if tag_col in df.columns:
        return df

    if '수익성계획전표번호' in df.columns:
        is_plan = df['수익성계획전표번호'].notnull() & (df['수익성계획전표번호'].astype(str).str.strip() != "")
        df.loc[is_plan, tag_col] = "판매계획"
        df.loc[~is_plan, tag_col] = "판매실적"
    else:
        df[tag_col] = "판매실적"
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

# --- 사이드바: 데이터 업로드 ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("데이터 통합 및 DB 최적화 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 처리
        for file in uploaded_plans:
            try:
                df = pd.read_excel(file, dtype={'매출처': str})
                df.columns = [str(c).strip() for c in df.columns]
                df = format_specific_columns(df)
                df = filter_invalid_rows(df, file.name)
                df = df.rename(columns={'품명': '품목명', '판매금액': '장부금액'})
                
                qty = pd.to_numeric(df.get('판매수량', 0), errors='coerce').fillna(0)
                price = pd.to_numeric(df.get('판매단가', 0), errors='coerce').fillna(0)
                df['판매금액'] = qty * price
                
                df = add_data_tag(df)
                all_data.append(df)
                st.write(f"✅ [계획] {file.name} 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 2] 판매실적 처리
        for file in uploaded_results:
            try:
                df = pd.read_excel(file, dtype={'매출처': str})
                df.columns = [str(c).strip() for c in df.columns]
                df = format_specific_columns(df)
                df = filter_invalid_rows(df, file.name)
                df = add_data_tag(df)
                all_data.append(df)
                st.write(f"✅ [실적] {file.name} 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 3] 기존 DB 로드
        for file in uploaded_dbs:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(file.getvalue())
                tmp_path = tmp_file.name
            try:
                conn_old = sqlite3.connect(tmp_path)
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                for target_table in tables['name']:
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    df_db = format_specific_columns(df_db)
                    all_data.append(df_db)
                conn_old.close()
                st.write(f"✅ [기존 DB] {file.name} 데이터 로드 완료")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 통합 데이터 최종 정제 (중복 컬럼 오류 방지)
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 1. 컬럼명 1차 정제 (특수문자 제거 및 이름 통일)
            raw_clean_names = []
            for col in combined_df.columns:
                # 특수문자를 언더바(_)로 바꾸고 연속된 언더바 제거
                clean_name = re.sub(r'[^a-zA-Z0-9가-힣]', '_', str(col)).strip('_')
                clean_name = re.sub(r'_+', '_', clean_name) 
                raw_clean_names.append(clean_name if clean_name else "unnamed")
            
            combined_df.columns = raw_clean_names

            # 2. ★ 핵심: 이름이 완전히 똑같아진 컬럼들은 하나로 병합 (중복 제거)
            # duplicated()는 첫 번째 컬럼 외의 중복들을 찾음
            combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

            # 3. 데이터 타입 및 결측치 클리닝
            obj_cols = combined_df.select_dtypes(include=['object']).columns
            for col in obj_cols:
                combined_df[col] = combined_df[col].fillna('').astype(str).replace(['nan', 'None'], '')
            
            combined_df = combined_df.drop_duplicates()

            # [Step 5] SQLite DB 저장
            db_filename = "sales_integrated_final.db"
            if os.path.exists(db_filename):
                try: os.remove(db_filename)
                except: pass
            
            conn_new = sqlite3.connect(db_filename)
            try:
                combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace", chunksize=1000)
                conn_new.close()
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    combined_df.to_excel(writer, index=False, sheet_name='TotalData')
                excel_data = output.getvalue()

                status.update(label="✅ 모든 통합 작업 완료!", state="complete", expanded=False)
                st.success(f"🎊 통합이 완료되었습니다! (총 행 수: **{len(combined_df):,}** 행)")
                st.subheader("📊 통합 데이터 미리보기 (상위 10행)")
                st.dataframe(combined_df.head(10))
                
                col1, col2 = st.columns(2)
                with col1:
                    with open(db_filename, "rb") as f:
                        st.download_button("💾 통합 SQLite DB 다운로드", data=f, file_name=db_filename, use_container_width=True)
                with col2:
                    st.download_button("📑 Excel 통합파일 다운로드", data=excel_data, file_name="sales_integrated_final.xlsx", use_container_width=True)
            except Exception as e:
                st.error(f"❌ DB 저장 중 오류 발생: {e}")
