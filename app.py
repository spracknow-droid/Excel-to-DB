import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import io
import processor as proc
import constants as const

st.set_page_config(page_title="Sales Data Integrator", layout="wide")
st.title("📊 판매 데이터 통합 관리 시스템")
st.info("💡 계획과 실적 데이터를 분리하여 저장하고 비교 분석 테이블을 생성합니다.")

# 데이터 저장용 리스트 분리
plan_data_list = []
result_data_list = []

# --- 사이드바: 데이터 업로드 [6, 7] ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("데이터 처리 및 분석 테이블 생성 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 처리 [7, 8]
        for file in uploaded_plans:
            try:
                df = pd.read_excel(file, dtype={'매출처': str, '품목코드': str})
                df.columns = [str(c).strip() for c in df.columns]
                df = df.rename(columns=const.PLAN_RENAME_MAP)
                if 'No' in df.columns:
                    df = df.dropna(subset=['No'])
                
                df = proc.format_specific_columns(df)
                df = proc.clean_date_columns(df)
                
                # 계획 전용 계산 로직 [8]
                qty = pd.to_numeric(df.get('수량', 0), errors='coerce').fillna(0)
                price = pd.to_numeric(df.get('판매단가', 0), errors='coerce').fillna(0)
                df['판매금액'] = qty * price
                
                plan_data_list.append(df)
                st.write(f"✅ [계획] {file.name}")
            except Exception as e: st.error(f"계획 파일 에러 ({file.name}): {e}")

        # [Step 2] 판매실적 처리 [9]
        for file in uploaded_results:
            try:
                df = pd.read_excel(file, dtype={'매출처': str, '품목': str})
                df.columns = [str(c).strip() for c in df.columns]
                if 'No' in df.columns:
                    df = df.dropna(subset=['No'])
                
                df = proc.format_specific_columns(df)
                df = proc.clean_date_columns(df)
                result_data_list.append(df)
                st.write(f"✅ [실적] {file.name}")
            except Exception as e: st.error(f"실적 파일 에러 ({file.name}): {e}")

        # [Step 3] 기존 DB 로드 및 분류 [10]
        for file in uploaded_dbs:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
                tmp_file.write(file.getvalue())
                tmp_path = tmp_file.name
            try:
                conn_old = sqlite3.connect(tmp_path)
                # 기존 DB의 테이블별로 분류하여 로드
                for table in ["plan_data", "result_data"]:
                    try:
                        df_db = pd.read_sql(f"SELECT * FROM {table}", conn_old)
                        if table == "plan_data": plan_data_list.append(df_db)
                        else: result_data_list.append(df_db)
                    except: pass
                conn_old.close()
                st.write(f"✅ [기존 DB] {file.name} 로드 완료")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 데이터 통합 및 분석 테이블 생성 [11]
        final_plan_df = proc.finalize_combined_df(plan_data_list)
        final_result_df = proc.finalize_combined_df(result_data_list)
        analysis_df = proc.create_analysis_df(final_plan_df, final_result_df)

        # [Step 5] DB 저장 및 다운로드 준비 [11, 12]
        if final_plan_df is not None or final_result_df is not None:
            conn_new = sqlite3.connect(const.DB_FILENAME)
            if final_plan_df is not None: final_plan_df.to_sql("plan_data", conn_new, index=False, if_exists="replace")
            if final_result_df is not None: final_result_df.to_sql("result_data", conn_new, index=False, if_exists="replace")
            if analysis_df is not None: analysis_df.to_sql("analysis_data", conn_new, index=False, if_exists="replace")
            conn_new.close()

            # 엑셀 멀티 시트 생성 [12]
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                if final_plan_df is not None: final_plan_df.to_excel(writer, sheet_name='Plan', index=False)
                if final_result_df is not None: final_result_df.to_excel(writer, sheet_name='Result', index=False)
                if analysis_df is not None: analysis_df.to_excel(writer, sheet_name='Analysis', index=False)
            excel_data = output.getvalue()

            status.update(label="✅ 처리 완료!", state="complete", expanded=False)
            
            # UI 출력 및 다운로드 [12]
            st.success("데이터가 성공적으로 통합되었습니다.")
            if analysis_df is not None:
                st.subheader("📊 계획 대비 실적 분석 (미리보기)")
                st.dataframe(analysis_df.head(10))

            c1, c2 = st.columns(2)
            with c1:
                with open(const.DB_FILENAME, "rb") as f:
                    st.download_button("💾 통합 DB 다운로드", data=f, file_name=const.DB_FILENAME, use_container_width=True)
            with c2:
                st.download_button("📑 통합 Excel 다운로드", data=excel_data, file_name=const.EXCEL_FILENAME, use_container_width=True)
