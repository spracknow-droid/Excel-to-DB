import streamlit as st
import pandas as pd
import sqlite3
import io
import processor as proc
import unifier      # 새로운 모듈 추가
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
        # [Step 1 & 2] 데이터 정제 로직 실행 (processor 활용) [8]
        for file in uploaded_plans:
            df = pd.read_excel(file)
            plan_list.append(proc.clean_date_columns(proc.format_specific_columns(df)))

        for file in uploaded_results:
            df = pd.read_excel(file)
            result_list.append(proc.clean_date_columns(proc.format_specific_columns(df)))

        # [Step 3] 개별 테이블 최종 확정 [6]
        final_plan_df = proc.finalize_combined_df(plan_list)
        final_result_df = proc.finalize_combined_df(result_list)
        
        # [Step 4] 통합 테이블 생성 (unifier 활용) [2, 6]
        total_df = unifier.create_unified_total_df(final_plan_df, final_result_df)

        # [Step 5 & 6] DB 및 엑셀 저장 로직 [7, 9]
        conn = sqlite3.connect(const.DB_FILENAME)
        if final_plan_df is not None: final_plan_df.to_sql("plan_data", conn, index=False, if_exists="replace")
        if final_result_df is not None: final_result_df.to_sql("result_data", conn, index=False, if_exists="replace")
        if total_df is not None: total_df.to_sql("total_data", conn, index=False, if_exists="replace")
        conn.close()

        # 엑셀 다운로드 파일 준비 [7, 9]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if final_plan_df is not None: final_plan_df.to_excel(writer, sheet_name='Plan_Original', index=False)
            if final_result_df is not None: final_result_df.to_excel(writer, sheet_name='Result_Original', index=False)
            if total_df is not None: total_df.to_excel(writer, sheet_name='Total_Integrated', index=False)
        excel_data = output.getvalue()
        status.update(label="✅ 통합 완료!", state="complete")

    # 다운로드 버튼 출력 [9]
    col1, col2 = st.columns(2)
    with col1:
        with open(const.DB_FILENAME, "rb") as f:
            st.download_button("💾 통합 DB 다운로드", data=f, file_name=const.DB_FILENAME, use_container_width=True)
    with col2:
        st.download_button("📑 통합 Excel 다운로드", data=excel_data, file_name=const.EXCEL_FILENAME, use_container_width=True)
