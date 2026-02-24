import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 Excel & SQLite 데이터 통합 변환기")
st.markdown("엑셀 파일들과 기존 DB 파일을 합쳐서 중복 없는 하나의 SQLite DB로 만듭니다.")

# --- 사이드바: 파일 업로드 영역 ---
st.sidebar.header("📁 파일 업로드")

# 1. 엑셀 파일 업로드 (다중)
uploaded_excels = st.sidebar.file_uploader(
    "1️⃣ xlsx 파일을 선택하세요", 
    type=["xlsx"], 
    accept_multiple_files=True
)

# 2. 기존 SQLite DB 업로드 (단일)
uploaded_db = st.sidebar.file_uploader(
    "2️⃣ 기존 SQLite (.db) 파일을 선택하세요", 
    type=["db"], 
    accept_multiple_files=False
)

# 데이터를 저장할 리스트 초기화
all_data = []

# --- 데이터 처리 로직 ---
if uploaded_excels or uploaded_db:
    with st.status("데이터 통합 및 처리 중...", expanded=True) as status:
        
        # [Step 1] 엑셀 파일 처리
        if uploaded_excels:
            for file in uploaded_excels:
                try:
                    df = pd.read_excel(file)
                    all_data.append(df)
                    st.write(f"✅ Excel 로드 완료: {file.name}")
                except Exception as e:
                    st.error(f"❌ {file.name} 읽기 실패: {e}")

        # [Step 2] 기존 DB 파일 처리
        if uploaded_db:
            # 업로드된 DB를 임시 파일로 저장 (sqlite3는 파일 경로가 필요함)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(uploaded_db.getvalue())
                tmp_path = tmp_file.name
            
            try:
                conn_old = sqlite3.connect(tmp_path)
                # DB 내부의 모든 테이블 목록 확인
                tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn_old)
                if not tables.empty:
                    # 첫 번째 테이블의 데이터를 가져옴
                    target_table = tables.iloc[0]['name']
                    df_db = pd.read_sql(f"SELECT * FROM {target_table}", conn_old)
                    all_data.append(df_db)
                    st.write(f"✅ 기존 DB 로드 완료: {uploaded_db.name} (테이블: {target_table})")
                else:
                    st.warning(f"⚠️ {uploaded_db.name} 내에 테이블이 존재하지 않습니다.")
                conn_old.close()
            except Exception as e:
                st.error(f"❌ DB 로드 실패: {e}")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        # [Step 3] 데이터 병합 및 타입 교정 (ProgrammingError 방지 핵심)
        if all_data:
            # 모든 데이터를 하나로 병합
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 🔥 중요: SQLite 저장 에러 방지 (Object 타입을 String으로 변환)
            # 엑셀의 복잡한 서식이나 리스트 형태의 데이터를 문자열로 처리합니다.
            for col in combined_df.columns:
                if combined_df[col].dtype == 'object':
                    combined_df[col] = combined_df[col].astype(str)
            
            # 중복 행 제거
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates()
            final_count = len(combined_df)
            
            st.write(f"📝 중복 제거 완료: {initial_count - final_count}행 삭제됨")

            # [Step 4] 최종 SQLite 파일 생성
            db_filename = "merged_database.db"
            if os.path.exists(db_filename):
                os.remove(db_filename)
                
            try:
                conn_new = sqlite3.connect(db_filename)
                # 데이터프레임을 DB 파일로 저장
                combined_df.to_sql("excel_data", conn_new, index=False, if_exists="replace")
                conn_new.close()
                
                status.update(label="통합 및 변환 성공!", state="complete", expanded=False)

                # --- 결과 표시 및 다운로드 ---
                st.subheader("📊 통합 데이터 미리보기 (상위 5행)")
                st.dataframe(combined_df.head())

                with open(db_filename, "rb") as f:
                    st.download_button(
                        label="💾 통합된 SQLite DB 다운로드",
                        data=f,
                        file_name=db_filename,
                        mime="application/octet-stream"
                    )
            except Exception as e:
                st.error(f"❌ DB 파일 생성 중 에러 발생: {e}")
        else:
            st.warning("처리할 데이터가 없습니다.")
else:
    st.info("왼쪽 사이드바에서 엑셀(.xlsx) 또는 데이터베이스(.db) 파일을 업로드해 주세요.")
