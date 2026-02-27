import streamlit as st
import pandas as pd
import sqlite3
import os
import io
from processor import clean_data
from view_manager import create_sales_views 

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

        safe_columns = [f'"{col}"' for col in df.columns]
        group_cols = ", ".join(safe_columns)
        try:
            conn.execute(f"DELETE FROM {target_table} WHERE rowid NOT IN (SELECT MIN(rowid) FROM {target_table} GROUP BY {group_cols})")
            conn.commit()
            
            # 전처리 View 생성 호출
            create_sales_views(conn)
            
            st.success(f"✅ {fname} 반영 및 전처리 완료")
        except sqlite3.OperationalError as e:
            st.error(f"⚠️ {fname} SQL 오류: {e}")

# --- 데이터 확인 ---
st.divider()
tab1, tab2, tab3 = st.tabs(["판매계획 원본", "매출리스트 원본", "🧹 전처리 통합 (Cleaned)"])

with tab1:
    try:
        df_p = pd.read_sql("SELECT * FROM sales_plan_data", conn)
        st.dataframe(df_p, use_container_width=True)
    except: st.info("데이터가 없습니다.")

with tab2:
    try:
        df_a = pd.read_sql("SELECT * FROM sales_actual_data", conn)
        st.dataframe(df_a, use_container_width=True)
    except: st.info("데이터가 없습니다.")

# 🚀 [핵심 수정] Tab 3: 호출 시 컬럼명을 강제로 지정하여 밀림 방지
with tab3:
    st.subheader("📋 매출리스트 컬럼명 기준 전처리 결과")
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.write("**[계획] 전처리 데이터**")
        try:
            # 판매계획 뷰에서 필요한 데이터를 매출리스트 형식으로 명시적 호출
            df_plan_clean = pd.read_sql("""
                SELECT 
                    기준월, 
                    매출처명, 
                    품명 AS 품목명, 
                    계획수량 AS 수량, 
                    계획금액_원화 AS 장부금액 
                FROM view_plan_vs_actual
            """, conn)
            st.dataframe(df_plan_clean, use_container_width=True)
        except:
            st.info("계획 데이터를 업로드해주세요.")
        
    with col_right:
        st.write("**[실적] 전처리 데이터**")
        try:
            # 실적 데이터 호출 시 '품명' 컬럼을 '품목명' 위치에 고정하여 밀림 해결
            df_actual_clean = pd.read_sql("""
                SELECT 
                    분석월 AS 기준월, 
                    매출처명, 
                    품명 AS 품목명, 
                    실적수량 AS 수량, 
                    실적금액_원화 AS 장부금액 
                FROM view_plan_vs_actual
            """, conn)
            st.dataframe(df_actual_clean, use_container_width=True)
        except:
            st.info("실적 데이터를 업로드해주세요.")

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
        try: pd.read_sql("SELECT * FROM sales_plan_data", conn).to_excel(writer, sheet_name='plan_raw', index=False)
        except: pass
        try: pd.read_sql("SELECT * FROM sales_actual_data", conn).to_excel(writer, sheet_name='actual_raw', index=False)
        except: pass
        
        # 엑셀 다운로드 시에도 컬럼명이 통일된 데이터를 포함
        try:
            df_p_clean = pd.read_sql("SELECT 기준월, 매출처명, 품명 AS 품목명, 계획수량 AS 수량, 계획금액_원화 AS 장부금액 FROM view_plan_vs_actual", conn)
            df_p_clean.to_excel(writer, sheet_name='plan_cleaned', index=False)
        except: pass
        
        try:
            df_a_clean = pd.read_sql("SELECT 분석월 AS 기준월, 매출처명, 품명 AS 품목명, 실적수량 AS 수량, 실적금액_원화 AS 장부금액 FROM view_plan_vs_actual", conn)
            df_a_clean.to_excel(writer, sheet_name='actual_cleaned', index=False)
        except: pass
        
    st.download_button("📊 Excel 통합 파일 다운로드", output.getvalue(), "cleaned_sales_data.xlsx")
