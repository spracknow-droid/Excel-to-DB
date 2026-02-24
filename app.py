import streamlit as st
import pandas as pd
import sqlite3
import os

st.set_page_config(page_title="Excel to SQLite Converter", layout="wide")

st.title("📊 Excel을 SQLite DB로 변환하기")
st.markdown("여러 개의 엑셀 파일을 하나로 합치고, 중복을 제거한 뒤 SQLite 파일로 변환합니다.")

# 1. 사이드바에서 파일 업로드
st.sidebar.header("파일 업로드")
uploaded_files = st.sidebar.file_uploader(
    "xlsx 파일을 선택하세요 (다중 선택 가능)", 
    type=["xlsx"], 
    accept_multiple_files=True
)

if uploaded_files:
    all_data = []
    
    with st.status("데이터 처리 중...", expanded=True) as status:
        # 엑셀 파일 읽기 및 병합
        for file in uploaded_files:
            df = pd.read_excel(file)
            all_data.append(df)
            st.write(f"✅ {file.name} 로드 완료")
        
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # 2. 중복 행 제거
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates()
        final_count = len(combined_df)
        
        st.write(f"📝 총 {initial_count}개 행 중 {initial_count - final_count}개의 중복 행을 제거했습니다.")
        
        # 3. SQLite 파일 생성
        db_filename = "converted_database.db"
        # 기존 파일이 있다면 삭제
        if os.path.exists(db_filename):
            os.remove(db_filename)
            
        conn = sqlite3.connect(db_filename)
        # 데이터프레임을 'excel_data'라는 테이블로 저장
        combined_df.to_sql("excel_data", conn, index=False, if_exists="replace")
        conn.close()
        
        status.update(label="변환 완료!", state="complete", expanded=False)

    # 미리보기 및 다운로드
    st.subheader("데이터 미리보기 (상위 5행)")
    st.dataframe(combined_df.head())

    with open(db_filename, "rb") as f:
        st.download_button(
            label="💾 SQLite DB 파일 다운로드",
            data=f,
            file_name=db_filename,
            mime="application/octet-stream"
        )
else:
    st.info("왼쪽 사이드바에서 엑셀 파일을 업로드해 주세요.")
