import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile

st.set_page_config(page_title="Data Merger (Excel & DB)", layout="wide")

st.title("📊 Excel & SQLite 데이터 통합 변환기")
st.markdown("엑셀 파일들과 기존 DB 파일을 합쳐서 중복 없는 하나의 SQLite DB로 만듭니다.")

# --- 사이드바: 파일 업로드 영역 ---
st.sidebar.header("📁 파일 업로드")

# 1. 엑셀 파일 업로드
uploaded_excels = st.sidebar.file_uploader(
    "1️⃣ xlsx 파일을 선택하세요", 
    type=["xlsx"], 
    accept_multiple_files=True
)

# 2. 기존 SQLite DB 업로드
uploaded_db = st.sidebar.file_uploader(
    "2️⃣ 기존 SQLite (.db) 파일을 선택하세요", 
    type=["db"], 
    accept_multiple_files=False
)

# 데이터 처리를 시작할 리스트
all_data = []

# --- 데이터 처리 로직 ---
if uploaded_excels or uploaded_db:
    with st.status("데이터 통합 및 처리 중...", expanded=True) as status:
        
        # [Step 1] 엑셀 파일 처리
        if uploaded_excels:
            for file in uploaded_excels:
                df = pd.read_excel(file)
                all_data.append(df)
                st.write(f"✅ Excel 로드 완료: {file.name}")

        # [Step 2] 기존 DB 파일 처리
        if uploaded_db:
            # Streamlit의 UploadedFile은 바로 sqlite3.connect에 넣을 수 없으므로 임시 파일로 저장
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(uploaded_db.getvalue())
                tmp_path = tmp_file.name
            
            try:
                conn_old = sqlite3.connect(tmp_path)
                # DB 안의 모든 테이블 중 첫 번째 테이블을 가져오거나, 특정 테이블명을 지정
                # 여기서는 'excel_data' 테이블이 있다고 가정하고 가져옵니다.
                # 만약 테이블 이름을 모른다면 전체 조회를 시도합니다.
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                if not tables.empty:
                    target_table = tables.iloc[0]['name'] # 첫 번째 테이블 선택
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    all_data.append(df_db)
                    st.write(f"✅ 기존 DB 로드 완료: {uploaded_db.name} (테이블: {target_table})")
                conn_old.close()
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        # [Step 3] 데이터 합치기 및 중복 제거
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates()
            final_count = len(combined_df)
            
            st.write(f"📝 데이터 통합 완료! (중복 제거: {initial_count - final_count}행 삭제)")

            # [Step 4] 최종 SQLite 파일 생성
            db_filename = "merged_database.db"
            if os.path.exists(db_filename):
                os.remove(db_filename)
                
            conn_new = sqlite3.connect(db_filename)
            combined_df.to_sql("excel_data", conn_new, index=False, if_exists="replace")
            conn_new.close()
            
            status.update(label="통합 및 변환 완료!", state="complete", expanded=False)

            # --- 결과 화면 ---
            st.subheader("📊 통합 데이터 미리보기 (상위 5행)")
            st.dataframe(combined_df.head())

            with open(db_filename, "rb") as f:
                st.download_button(
                    label="💾 통합된 SQLite DB 다운로드",
                    data=f,
                    file_name=db_filename,
                    mime="application/octet-stream"
                )
        else:
            st.warning("데이터를 불러오지 못했습니다. 파일 형식을 확인해 주세요.")
else:
    st.info("왼쪽 사이드바에서 엑셀 또는 DB 파일을 업로드해 주세요.")
