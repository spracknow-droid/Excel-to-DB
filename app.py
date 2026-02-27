import streamlit as st
import pandas as pd
import sqlite3
import io
import processor as proc
import unifier
import constants as const

st.set_page_config(page_title="Sales Data Integrator", layout="wide")
st.title("📊 판매 데이터 통합 시스템")

plan_list = []
result_list = []

st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)

if uploaded_plans or uploaded_results:
    with st.status("데이터 처리 및 통합 중...") as status:
        # [Step 1] 판매계획 데이터 정제
        for file in uploaded_plans:
            df = pd.read_excel(file)
            
            # 1. 컬럼명 표준화 (constants.py의 PLAN_RENAME_MAP 활용)
            df = df.rename(columns=const.PLAN_RENAME_MAP)
            
            # 2. '매출번호'가 '합계'인 행 제거
            df = proc.remove_total_rows(df)
            
            # 3. 데이터 포맷팅 (문자열 변환, 날짜 정제 등)
            df = proc.format_specific_columns(df)
            df = proc.clean_date_columns(df)
            
            plan_list.append(df)

        # [Step 2] 판매실적 데이터 정제
        for file in uploaded_results:
            df = pd.read_excel(file)
            
            # 1. '매출번호'가 '합계'인 행 제거
            df = proc.remove_total_rows(df)
            
            # 2. 데이터 포맷팅
            df = proc.format_specific_columns(df)
            df = proc.clean_date_columns(df)
            
            result_list.append(df)

        # [Step 3] 개별 테이블 최종 확정 (리스트 합치기 및 컬럼명 특수문자 정제)
        final_plan_df = proc.finalize_combined_df(plan_list)
        final_result_df = proc.finalize_combined_df(result_list)
        
        # [Step 4] 통합 테이블 생성 (unifier 활용)
        total_df = unifier.create_unified_total_df(final_plan_df, final_result_df)

        # [Step 5] DB 저장 (SQLite)
        conn = sqlite3.connect(const.DB_FILENAME)
        if final_plan_df is not None: 
            final_plan_df.to_sql("plan_data", conn, index=False, if_exists="replace")
        if final_result_df is not None: 
            final_result_df.to_sql("result_data", conn, index=False, if_exists="replace")
        if total_df is not None: 
            total_df.to_sql("total_data", conn, index=False, if_exists="replace")
        conn.close()

        # [Step 6] 엑셀 다운로드 파일 준비
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if final_plan_df is not None: 
                final_plan_df.to_excel(writer, sheet_name='Plan_Original', index=False)
            if final_result_df is not None: 
                final_result_df.to_excel(writer, sheet_name='Result_Original', index=False)
            if total_df is not None: 
                total_df.to_excel(writer, sheet_name='Total_Integrated', index=False)
        excel_data = output.getvalue()
        
        status.update(label="✅ 데이터 통합 및 정제 완료!", state="complete")

    # 화면 표시 및 다운로드 버튼
    col1, col2 = st.columns(2)
    with col1:
        if final_plan_df is not None:
            st.subheader("📋 정제된 판매계획 (상위 5행)")
            st.dataframe(final_plan_df.head())
    with col2:
        if final_result_df is not None:
            st.subheader("📈 정제된 판매실적 (상위 5행)")
            st.dataframe(final_result_df.head())

    st.divider()
    st.subheader("🔗 통합 데이터 미리보기")
    if total_df is not None:
        st.dataframe(total_df.head(10))
        
        st.download_button(
            label="📂 통합 데이터 엑셀 다운로드",
            data=excel_data,
            file_name=const.EXCEL_FILENAME,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
else:
    st.info("왼쪽 사이드바에서 판매계획 또는 판매실적 파일을 업로드해주세요.")
