import streamlit as st
import pandas as pd
import sqlite3
import os
import io
from processor import clean_data
from view_manager import create_sales_views  # 🚀 추가: View 생성 함수 임포트

# --- 💡 세션 기반 메모리 DB 초기화 ---
if 'db_conn' not in st.session_state:
    st.session_state.db_conn = sqlite3.connect(':memory:', check_same_thread=False)

conn = st.session_state.db_conn

st.set_page_config(page_title="데이터 통합 도구", layout="wide")
st.title("🔋 세션 기반 실시간 데이터 통합")

# --- 사이드바 ---
with st.sidebar:
    st.header("📂 데이터 업로드")
    excel_files = st.file_uploader(
        "1️⃣ 시스템 엑셀 파일 (SLSSPN / BILBIV)", 
        type=["xlsx", "xls"], 
        accept_multiple_files=True
    )
    st.divider()
    uploaded_db = st.file_uploader("2️⃣ 기존 SQLite DB 파일 (.db)", type=["db"])

# --- 로직 1: 업로드된 DB 파일 처리 ---
if uploaded_db:
    with open("temp_uploaded.db", "wb") as f:
        f.write(uploaded_db.getbuffer())
    with sqlite3.connect("temp_uploaded.db") as temp_conn:
        temp_conn.backup(st.session_state.db_conn)
    os.remove("temp_uploaded.db")
    
    # DB 로드 후 View 업데이트
    try:
        create_sales_views(st.session_state.db_conn)
    except:
        pass
        
    st.sidebar.success("✅ DB 파일 로드 및 View 업데이트 완료")

# --- 로직 2: 엑셀 파일 처리 ---
if excel_files:
    for file in excel_files:
        fname = file.name
        
        str_converters = {}
        if "SLSSPN" in fname:
            target_table = "sales_plan_data"
            target_type = "SLSSPN"
            str_converters = {'매출처': str, '품목코드': str}
        elif "BILBIV" in fname:
            target_table = "sales_actual_data"
            target_type = "BILBIV"
            str_converters = {'매출처': str, '품목': str, '수금처': str, '납품처': str}
        else:
            continue

        df = pd.read_excel(file, converters=str_converters)
        df = clean_data(df, target_type)

        if target_type == "BILBIV" and '매출번호' in df.columns:
            df = df[df['매출번호'].astype(str).str.contains('합계') == False]

        try:
            existing_columns = pd.read_sql(f"SELECT * FROM {target_table} LIMIT 0", conn).columns.tolist()
            for col in existing_columns:
                if col not in df.columns:
                    df[col] = None
            if existing_columns:
                df = df[existing_columns]
            df.to_sql(target_table, conn, if_exists="append", index=False)
        except Exception:
            df.to_sql(target_table, conn, if_exists="replace", index=False)

        # SQL 기반 중복 제거
        safe_columns = [f'"{col}"' for col in df.columns]
        group_cols = ", ".join(safe_columns)
        try:
            conn.execute(f"DELETE FROM {target_table} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {target_table} GROUP BY {group_cols})")
            conn.commit()
            
            # 🚀 [핵심 추가] 데이터 업로드 후 분석 View 생성/업데이트 호출
            create_sales_views(conn)
            
            st.success(f"✅ {fname} 반영 및 분석 View 업데이트 완료")
        except sqlite3.OperationalError as e:
            st.error(f"⚠️ {fname} SQL 오류: {e}")

# --- 데이터 확인 ---
st.divider()
# 🚀 탭 추가: 분석 View 탭을 세 번째에 배치
tab1, tab2, tab3 = st.tabs(["판매계획 (Sales Plan)", "매출리스트 (Sales Actual)", "📊 분석 View (Plan vs Actual)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM sales_plan_data", conn)
        if not df_p.empty:
            st.write(f"현재 데이터: **{len(df_p)}** 행")
            st.dataframe(df_p, use_container_width=True)
        else: st.info("데이터가 비어있습니다.")
    except: st.info("데이터가 없습니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM sales_actual_data", conn)
        if not df_a.empty:
            st.write(f"현재 데이터: **{len(df_a)}** 행")
            st.dataframe(df_a, use_container_width=True)
        else: st.info("데이터가 비어있습니다.")
    except: st.info("데이터가 없습니다.")

# 🚀 [추가] 분석 View 탭 로직
with tab3:
    st.subheader("📈 계획 대비 실적 분석 (장부금액 기준)")
    try:
        # view_manager에서 생성한 view 조회
        df_v = pd.read_sql("SELECT * FROM view_plan_vs_actual ORDER BY 분석월 DESC", conn)
        if not df_v.empty:
            # 수치 가독성을 위해 스타일링 (옵션)
            st.dataframe(df_v.style.format({
                '계획수량': '{:,.0f}', '실적수량': '{:,.0f}', '수량차이': '{:,.0f}',
                '계획금액_원화': '{:,.0f}', '실적금액_원화': '{:,.0f}', '금액차이_원화': '{:,.0f}',
                '매출달성률': '{:.1f}%'
            }), use_container_width=True)
        else:
            st.info("분석할 데이터가 충분하지 않습니다. 계획과 실적 파일을 모두 업로드해주세요.")
    except:
        st.info("데이터 업로드 시 분석 View가 자동으로 생성됩니다.")

# --- 내보내기 ---
st.divider()
col1, col2 = st.columns(2)
with col1:
    temp_db_path = "export.db"
    with sqlite3.connect(temp_db_path) as export_conn:
        st.session_state.db_conn.backup(export_conn)
    with open(temp_db_path, "rb") as f:
        st.download_button("💾 SQLite DB 다운로드", f, "integrated_data.db")
    if os.path.exists(temp_db_path): os.remove(temp_db_path)

with col2:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        try: pd.read_sql("SELECT * FROM sales_plan_data", conn).to_excel(writer, sheet_name='sales_plan_data', index=False)
        except: pass
        try: pd.read_sql("SELECT * FROM sales_actual_data", conn).to_excel(writer, sheet_name='sales_actual_data', index=False)
        except: pass
        # 🚀 [추가] 분석 View 결과도 엑셀 시트로 포함
        try: pd.read_sql("SELECT * FROM view_plan_vs_actual", conn).to_excel(writer, sheet_name='Analysis_View', index=False)
        except: pass
        
    st.download_button("📊 Excel 통합 파일 다운로드", output.getvalue(), "integrated_data.xlsx")
