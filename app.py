import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 통합 데이터 변환기 (계획 & 실적 & DB)")
st.markdown("사이드바에서 각 카테고리에 맞는 파일들을 업로드해 주세요.")

# --- 사이드바: 3개 업로드 섹션 ---
st.sidebar.header("📁 데이터 소스 업로드")

# 1) 판매계획 (엑셀, 다중 가능)
uploaded_plans = st.sidebar.file_uploader(
    "1️⃣ 판매계획 (xlsx)", 
    type=["xlsx"], 
    accept_multiple_files=True,
    key="plan_uploader"
)

# 2) 판매실적 (엑셀, 다중 가능)
uploaded_results = st.sidebar.file_uploader(
    "2️⃣ 판매실적 (xlsx)", 
    type=["xlsx"], 
    accept_multiple_files=True,
    key="result_uploader"
)

# 3) SQLite DB (DB, 다중 가능)
uploaded_dbs = st.sidebar.file_uploader(
    "3️⃣ 기존 SQLite (db)", 
    type=["db"], 
    accept_multiple_files=True,
    key="db_uploader"
)

# 데이터를 통합 저장할 리스트
all_data = []

# --- 데이터 처리 로직 ---
if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("파일 읽기 및 통합 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 처리
        for file in uploaded_plans:
            try:
                df = pd.read_excel(file)
                df['__데이터구분__'] = "판매계획" # 추후 필터링을 위한 구분값
                all_data.append(df)
                st.write(f"✅ [계획] {file.name} 완료")
            except Exception as e:
                st.error(f"❌ {file.name} 읽기 실패: {e}")

        # [Step 2] 판매실적 처리
        for file in uploaded_results:
            try:
                df = pd.read_excel(file)
                df['__데이터구분__'] = "판매실적"
                all_data.append(df)
                st.write(f"✅ [실적] {file.name} 완료")
            except Exception as e:
                st.error(f"❌ {file.name} 읽기 실패: {e}")

        # [Step 3] SQLite DB 처리 (다중 DB 대응)
        for file in uploaded_dbs:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(file.getvalue())
                tmp_path = tmp_file.name
            
            try:
                conn_old = sqlite3.connect(tmp_path)
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                for target_table in tables['name']: # DB 안의 모든 테이블 순회
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    df_db['__데이터구분__'] = f"DB({file.name}_{target_table})"
                    all_data.append(df_db)
                st.write(f"✅ [DB] {file.name} 로드 완료")
                conn_old.close()
            except Exception as e:
                st.error(f"❌ {file.name} 로드 실패: {e}")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        # [Step 4] 병합 및 중복 제거
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # SQLite 호환성을 위한 타입 변환 (Object -> String)
            for col in combined_df.columns:
                if combined_df[col].dtype == 'object':
                    combined_df[col] = combined_df[col].astype(str)
            
            # 중복 제거
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates()
            final_count = len(combined_df)
            
            st.write(f"📝 전체 통합 결과: {final_count}행 (중복 {initial_count - final_count}행 삭제됨)")

            # [Step 5] 다운로드 파일 생성
            db_filename = "integrated_sales_data.db"
            if os.path.exists(db_filename):
                os.remove(db_filename)
                
            conn_new = sqlite3.connect(db_filename)
            combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace")
            conn_new.close()
            
            status.update(label="통합 완료!", state="complete", expanded=False)

            # 결과 미리보기 및 다운로드 버튼
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
    st.info("사이드바에서 분석할 파일들을 업로드해 주세요.")
