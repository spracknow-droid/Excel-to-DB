import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 통합 데이터 변환기 (전표번호 기준 분류)")
st.markdown("'수익성계획전표번호'에 값이 있으면 판매계획(품명/판매금액 변경 및 신규 금액 계산), 없으면 판매실적으로 분류합니다.")

# --- 사이드바: 3개 업로드 섹션 ---
st.sidebar.header("📁 데이터 소스 업로드")

uploaded_plans = st.sidebar.file_uploader(
    "1️⃣ 판매계획 (xlsx)", 
    type=["xlsx"], 
    accept_multiple_files=True,
    key="plan_uploader"
)

uploaded_results = st.sidebar.file_uploader(
    "2️⃣ 판매실적 (xlsx)", 
    type=["xlsx"], 
    accept_multiple_files=True,
    key="result_uploader"
)

uploaded_dbs = st.sidebar.file_uploader(
    "3️⃣ 기존 SQLite (db)", 
    type=["db"], 
    accept_multiple_files=True,
    key="db_uploader"
)

all_data = []

# 데이터 처리 함수
def process_classification(df):
    if '수익성계획전표번호' in df.columns:
        # 값이 있는 경우 (판매계획)
        is_plan = df['수익성계획전표번호'].notnull() & (df['수익성계획전표번호'].astype(str).str.strip() != "")
        
        # [판매계획 데이터 처리]
        df_plan = df[is_plan].copy()
        if not df_plan.empty:
            # 1. 컬럼명 변경 (기존 판매금액 -> 장부금액)
            df_plan = df_plan.rename(columns={'품명': '품목명', '판매금액': '장부금액'})
            
            # 2. 신규 판매금액 생성 (판매수량 * 판매단가)
            # 수치 데이터가 아닌 경우를 대비해 숫자로 변환 후 계산
            qty = pd.to_numeric(df_plan.get('판매수량', 0), errors='coerce').fillna(0)
            price = pd.to_numeric(df_plan.get('판매단가', 0), errors='coerce').fillna(0)
            df_plan['판매금액'] = qty * price
            
            df_plan['__데이터구분__'] = "판매계획"
            all_data.append(df_plan)
            
        # [판매실적 데이터 처리]
        df_result = df[~is_plan].copy()
        if not df_result.empty:
            df_result['__데이터구분__'] = "판매실적"
            all_data.append(df_result)
    else:
        # 컬럼이 없으면 기본적으로 판매실적으로 분류
        df['__데이터구분__'] = "판매실적"
        all_data.append(df)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("파일 읽기 및 분류 로직 적용 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 섹션 파일 처리
        for file in uploaded_plans:
            try:
                df = pd.read_excel(file)
                process_classification(df)
                st.write(f"✅ [계획섹션] {file.name} 처리 완료")
            except Exception as e:
                st.error(f"❌ {file.name} 읽기 실패: {e}")

        # [Step 2] 판매실적 섹션 파일 처리
        for file in uploaded_results:
            try:
                df = pd.read_excel(file)
                process_classification(df)
                st.write(f"✅ [실적섹션] {file.name} 처리 완료")
            except Exception as e:
                st.error(f"❌ {file.name} 읽기 실패: {e}")

        # [Step 3] SQLite DB 처리
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
                st.write(f"✅ [DB] {file.name} 로드 및 분류 완료")
                conn_old.close()
            except Exception as e:
                st.error(f"❌ {file.name} 로드 실패: {e}")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        # [Step 4] 병합 및 중복 제거
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            for col in combined_df.columns:
                if combined_df[col].dtype == 'object':
                    combined_df[col] = combined_df[col].astype(str)
            
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates()
            final_count = len(combined_df)
            
            st.write(f"📝 전체 통합 결과: {final_count}행 (중복 {initial_count - final_count}행 삭제됨)")

            # [Step 5] DB 파일 생성
            db_filename = "integrated_sales_data.db"
            if os.path.exists(db_filename):
                os.remove(db_filename)
                
            conn_new = sqlite3.connect(db_filename)
            combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace")
            conn_new.close()
            
            status.update(label="통합 완료!", state="complete", expanded=False)

            st.subheader("📊 통합 데이터 미리보기")
            st.dataframe(combined_df.head(10))

            with open(db_filename, "rb") as f:
                st.download_button(
                    label="💾 통합된 SQLite DB 다운로드",
                    data=f,
                    file_name=db_filename,
                    mime="application/octet-stream"
                )
else:
    st.info("사이드바에서 파일을 업로드해 주세요.")
