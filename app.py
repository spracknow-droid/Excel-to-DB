import streamlit as st
import pandas as pd
import sqlite3
import os
import tempfile
import re
import io

# 페이지 설정
st.set_page_config(page_title="Excel & DB Merger", layout="wide")

st.title("📊 판매 데이터(계획/실적) SQLite DB 변환기")
st.info("💡 사용자가 업로드한 판매 데이터(계획/실적)를 통합하여 SQLite DB로 변환하는 페이지입니다.")

all_data = []

# [함수] 특정 컬럼 타입을 문자열로 고정 및 데이터 클리닝
def format_specific_columns(df):
    """'매출처' 등 코드 성격의 컬럼을 깨끗한 문자열 형식으로 변환"""
    target_cols = ['매출처', '품목명', '품번'] # 변환이 필요한 주요 컬럼들
    for col in target_cols:
        if col in df.columns:
            # 1. 모든 데이터를 문자열로 변환하고 결측치 처리
            df[col] = df[col].astype(str).replace(['nan', 'None', 'nan.0'], '')
            # 2. 소수점(.0)으로 끝나는 숫자형 문자열 처리 (예: 12345.0 -> 12345)
            df[col] = df[col].apply(lambda x: x.split('.')[0] if x.endswith('.0') else x)
            # 3. 공백 제거
            df[col] = df[col].str.strip()
    return df

# [공통 로직] 데이터 구분(Tagging) 함수
def add_data_tag(df):
    if df is None or df.empty:
        return df
    
    if '수익성계획전표번호' in df.columns:
        # 전표번호가 있는 행은 '판매계획', 없는 행은 '판매실적'으로 태깅
        is_plan = df['수익성계획전표번호'].notnull() & (df['수익성계획전표번호'].astype(str).str.strip() != "")
        df.loc[is_plan, '__데이터구분__'] = "판매계획"
        df.loc[~is_plan, '__데이터구분__'] = "판매실적"
    else:
        df['__데이터구분__'] = "판매실적"
    return df

# [공통 로직] 'No' 컬럼 기반 유효 데이터 필터링 함수
def filter_invalid_rows(df, filename):
    if 'No' in df.columns:
        initial_len = len(df)
        df = df.dropna(subset=['No'])
        df = df[df['No'].astype(str).str.strip() != ""]
        final_len = len(df)
        
        if initial_len > final_len:
            st.warning(f"⚠️ {filename}: 'No' 값이 없는 {initial_len - final_len}개의 행이 제외되었습니다.")
        return df.reset_index(drop=True)
    return df

# --- 사이드바: 3개 업로드 섹션 ---
st.sidebar.header("📁 데이터 소스 업로드")
uploaded_plans = st.sidebar.file_uploader("1️⃣ 판매계획 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_results = st.sidebar.file_uploader("2️⃣ 판매실적 (xlsx)", type=["xlsx"], accept_multiple_files=True)
uploaded_dbs = st.sidebar.file_uploader("3️⃣ 기존 SQLite (db)", type=["db"], accept_multiple_files=True)

if uploaded_plans or uploaded_results or uploaded_dbs:
    with st.status("데이터 통합 및 DB 최적화 중...", expanded=True) as status:
        
        # [Step 1] 판매계획 처리
        for file in uploaded_plans:
            try:
                # 읽기 단계에서 '매출처' 타입을 str로 시도
                df = pd.read_excel(file, dtype={'매출처': str})
                df.columns = [str(c).strip() for c in df.columns]
                df = format_specific_columns(df)
                df = filter_invalid_rows(df, file.name)
                df = df.rename(columns={'품명': '품목명', '판매금액': '장부금액'})
                
                # 수량/단가 기반 계산 (금액 컬럼 생성)
                qty = pd.to_numeric(df.get('판매수량', 0), errors='coerce').fillna(0)
                price = pd.to_numeric(df.get('판매단가', 0), errors='coerce').fillna(0)
                df['판매금액'] = qty * price
                
                df = add_data_tag(df)
                all_data.append(df)
                st.write(f"✅ [계획] {file.name} 처리 완료")
            except Exception as e: st.error(f"Error ({file.name}): {e}")

        # [Step 2] 판매실적 처리
        for file in uploaded_results:
            try:
                df = pd.read_excel(file, dtype={'매출처': str})
                df.columns = [str(c).strip() for c in df.columns]
                df = format_specific_columns(df)
                df = filter_invalid_rows(df, file.name)
                df = add_data_tag(df)
                all_data.append(df)
                st.write(f"✅ [실적] {file.name} 처리 완료")
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
                    df_db = format_specific_columns(df_db) # DB 데이터도 매출처 포맷팅
                    all_data.append(df_db)
                conn_old.close()
                st.write(f"✅ [기존 DB] {file.name} 데이터 로드 완료")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

        # [Step 4] 통합 데이터 최종 정제 (OperationalError 방어막)
        if all_data:
            # 데이터 병합
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 1. 컬럼명 정제: 특수문자 제거 및 SQLite 호환 이름으로 변경
            clean_column_names = []
            for col in combined_df.columns:
                # 한글, 영문, 숫자 외에는 모두 언더바(_)로 변경
                clean_name = re.sub(r'[^a-zA-Z0-9가-힣]', '_', str(col)).strip('_')
                clean_column_names.append(clean_name)
            
            # 2. 중복 컬럼명 처리 (예: 매출처, 매출처_1)
            final_cols = []
            counts = {}
            for col in clean_column_names:
                if col in counts:
                    counts[col] += 1
                    final_cols.append(f"{col}_{counts[col]}")
                else:
                    counts[col] = 0
                    final_cols.append(col)
            combined_df.columns = final_cols

            # 3. 최종 데이터 타입 클리닝 (문자열 컬럼 내 결측치 제거)
            # 모든 Object 컬럼에 대해 결측치를 빈 문자열로 바꾸고 타입을 str로 확정
            obj_cols = combined_df.select_dtypes(include=['object']).columns
            for col in obj_cols:
                combined_df[col] = combined_df[col].fillna('').astype(str).replace(['nan', 'None'], '')
            
            combined_df = combined_df.drop_duplicates()

            # [Step 5] SQLite DB 저장
            db_filename = "sales_integrated_final.db"
            
            # 기존 파일 제거 시도
            if os.path.exists(db_filename):
                try: os.remove(db_filename)
                except: pass
            
            conn_new = sqlite3.connect(db_filename)
            try:
                # chunksize를 설정하여 대량 데이터 처리 시 안정성 확보
                combined_df.to_sql("total_data", conn_new, index=False, if_exists="replace", chunksize=1000)
                conn_new.close()
                
                # Excel 다운로드용 버퍼 생성
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    combined_df.to_excel(writer, index=False, sheet_name='TotalData')
                excel_data = output.getvalue()

                status.update(label="✅ 모든 통합 작업 완료!", state="complete", expanded=False)
                st.success(f"🎊 통합이 완료되었습니다! (총 행 수: **{len(combined_df):,}** 행)")
                
                # 결과 미리보기
                st.subheader("📊 통합 데이터 미리보기 (상위 10행)")
                st.dataframe(combined_df.head(10))
                
                # 다운로드 버튼
                col1, col2 = st.columns(2)
                with col1:
                    with open(db_filename, "rb") as f:
                        st.download_button(
                            "💾 통합 SQLite DB 다운로드", 
                            data=f, 
                            file_name=db_filename, 
                            mime="application/octet-stream",
                            use_container_width=True
                        )
                with col2:
                    st.download_button(
                        "📑 Excel 통합파일 다운로드", 
                        data=excel_data, 
                        file_name="sales_integrated_final.xlsx", 
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
            except Exception as e:
                st.error(f"❌ DB 저장 중 오류 발생: {e}")
                st.info("💡 팁: 컬럼명에 너무 많은 특수문자가 있거나 데이터 형식이 충돌할 때 발생할 수 있습니다.")
