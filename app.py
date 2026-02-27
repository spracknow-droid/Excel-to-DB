import streamlit as st
import pandas as pd
import sqlite3
import os
import io
import processor as proc
import constants as const

st.set_page_config(page_title="Sales Data Integrator", layout="wide")
st.title("📊 판매 데이터 통합 및 원본 보존 시스템")

# 각각의 데이터를 담을 리스트
plan_list = []
result_list = []

# --- 사이드바: 데이터 업로드 [5, 6] ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)

if uploaded_plans or uploaded_results:
    with st.status("데이터 처리 중...") as status:
        
        # [Step 1] 판매계획 원본 처리 [6, 7]
        for file in uploaded_plans:
            df = pd.read_excel(file)
            df = proc.format_specific_columns(df)
            df = proc.clean_date_columns(df)
            plan_list.append(df)
            st.write(f"✅ [계획 원본] {file.name}")

        # [Step 2] 판매실적 원본 처리 [8]
        for file in uploaded_results:
            df = pd.read_excel(file)
            df = proc.format_specific_columns(df)
            df = proc.clean_date_columns(df)
            result_list.append(df)
            st.write(f"✅ [실적 원본] {file.name}")

        # [Step 3] 개별 통합 데이터 생성 [3]
        final_plan_df = proc.finalize_combined_df(plan_list)
        final_result_df = proc.finalize_combined_df(result_list)
        
        # [Step 4] 통합용 테이블(total_data) 생성
        total_df = proc.create_unified_total_df(final_plan_df, final_result_df)

        # [Step 5] DB 저장 (각각의 테이블명으로 저장) [9]
        conn = sqlite3.connect(const.DB_FILENAME)
        if final_plan_df is not None:
            final_plan_df.to_sql("plan_data", conn, index=False, if_exists="replace")
        if final_result_df is not None:
            final_result_df.to_sql("result_data", conn, index=False, if_exists="replace")
        if total_df is not None:
            total_df.to_sql("total_data", conn, index=False, if_exists="replace")
        conn.close()

        # [Step 6] 엑셀 다운로드 파일 구성 (시트 분리) [9]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if final_plan_df is not None: final_plan_df.to_excel(writer, sheet_name='Plan_Original', index=False)
            if final_result_df is not None: final_result_df.to_excel(writer, sheet_name='Result_Original', index=False)
            if total_df is not None: total_df.to_excel(writer, sheet_name='Total_Integrated', index=False)
        excel_data = output.getvalue()

        status.update(label="✅ 처리 완료!", state="complete")

    # 다운로드 UI [10]
    st.success("원본 데이터 보존 및 통합 테이블 생성이 완료되었습니다.")
    col1, col2 = st.columns(2)
    with col1:
        with open(const.DB_FILENAME, "rb") as f:
            st.download_button("💾 통합 DB 다운로드", data=f, file_name=const.DB_FILENAME, use_container_width=True)
    with col2:
        st.download_button("📑 통합 Excel 다운로드", data=excel_data, file_name=const.EXCEL_FILENAME, use_container_width=True)
