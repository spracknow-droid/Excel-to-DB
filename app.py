import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import io
import processor as proc
import constants as const

st.set_page_config(page_title="Excel & DB Merger", layout="wide")
st.title("📊 판매 데이터(계획/실적) SQLite DB 변환기")
st.info("💡 업로드한 데이터를 통합하여 SQLite DB와 엑셀로 변환합니다.")

all_data = []

# --- 사이드바: 데이터 업로드 ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("데이터 통합 및 최적화 진행 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 처리
        for file in uploaded_plans:
            try:
                df = pd.read_excel(file, dtype={'매출처': str, '품목코드': str})
                df.columns = [str(c).strip() for c in df.columns]
                df = df.rename(columns=const.PLAN_RENAME_MAP)
                
                # 유효 행 필터링 로직
                if 'No' in df.columns:
                    df = df.dropna(subset=['No'])
                    df = df[df['No'].astype(str).str.strip() != ""]

                df = proc.format_specific_columns(df)
                df = proc.clean_date_columns(df)
                
                # 수량/금액 계산 로직
                qty = pd.to_numeric(df.get('수량', 0), errors='coerce').fillna(0)
                book_amt = pd.to_numeric(df.get('장부금액', 0), errors='coerce').fillna(0)
                df['장부단가'] = (book_amt / qty.replace(0, pd.NA)).fillna(0)
                price = pd.to_numeric(df.get('판매단가', 0), errors='coerce').fillna(0)
                df['판매금액'] = qty * price
                
                all_data.append(proc.add_data_tag(df))
                st.write(f"✅ [계획] {file.name}")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 2] 판매실적 처리
        for file in uploaded_results:
            try:
                df = pd.read_excel(file, dtype={'매출처': str, '수금처': str, '납품처': str, '품목': str})
                df.columns = [str(c).strip() for c in df.columns]
                
                if 'No' in df.columns:
                    df = df.dropna(subset=['No']).reset_index(drop=True)

                df = proc.format_specific_columns(df)
                df = proc.clean_date_columns(df)
                all_data.append(proc.add_data_tag(df))
                st.write(f"✅ [실적] {file.name}")
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
                    df_db = proc.format_specific_columns(df_db)
                    df_db = proc.clean_date_columns(df_db)
                    all_data.append(df_db)
                conn_old.close()
                st.write(f"✅ [기존 DB] {file.name}")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 통합 및 저장
        combined_df = proc.finalize_combined_df(all_data)
        
        if combined_df is not None:
            if os.path.exists(const.DB_FILENAME):
                try: os.remove(const.DB_FILENAME)
                except: pass
            
            conn_new = sqlite3.connect(const.DB_FILENAME)
            try:
                combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace", chunksize=1000)
                conn_new.close()
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    combined_df.to_excel(writer, index=False, sheet_name='TotalData')
                excel_data = output.getvalue()

                status.update(label="✅ 통합 완료!", state="complete", expanded=False)
                st.success(f"🎊 총 **{len(combined_df):,}** 행의 데이터가 통합되었습니다.")
                st.dataframe(combined_df.head(10))
                
                c1, c2 = st.columns(2)
                with c1:
                    with open(const.DB_FILENAME, "rb") as f:
                        st.download_button("💾 통합 DB 다운로드", data=f, file_name=const.DB_FILENAME, use_container_width=True)
                with c2:
                    st.download_button("📑 통합 Excel 다운로드", data=excel_data, file_name=const.EXCEL_FILENAME, use_container_width=True)
            except Exception as e:
                st.error(f"❌ 저장 중 오류: {e}")
