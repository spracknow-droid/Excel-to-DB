### 실제 데이터 연산과 정제를 담당하는 엔진입니다. Streamlit 함수(st.warning 등)는 UI 파일로 분리하고 순수 로직만 담았습니다. ###

import pandas as pd
import re
import constants as const

def format_specific_columns(df):
    """코드 성격의 컬럼을 문자열로 변환 및 .0 제거"""
    for col in const.TARGET_STR_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'nan.0'], '')
            df[col] = df[col].apply(lambda x: x.split('.')[0] if x.endswith('.0') else x).str.strip()
    return df

def clean_date_columns(df):
    """날짜 컬럼에서 시간 정보 제거 (YYYY-MM-DD)"""
    for col in const.TARGET_DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    return df

def add_data_tag(df):
    """판매계획/실적 구분 태그 추가"""
    if df is None or df.empty: return df
    tag_col = '__데이터구분__'
    if tag_col in df.columns: return df

    if '수익성계획전표번호' in df.columns:
        is_plan = df['수익성계획전표번호'].notnull() & (df['수익성계획전표번호'].astype(str).str.strip() != "")
        df.loc[is_plan, tag_col] = "판매계획"
        df.loc[~is_plan, tag_col] = "판매실적"
    else:
        df[tag_col] = "판매실적"
    return df

def finalize_combined_df(all_data):
    """통합된 데이터프레임의 컬럼명 정제 및 중복 제거"""
    if not all_data: return None
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # 1. 컬럼명 특수문자 제거 및 정제
    clean_names = []
    for col in combined_df.columns:
        c_name = re.sub(r'[^a-zA-Z0-9가-힣]', '_', str(col)).strip('_')
        c_name = re.sub(r'_+', '_', c_name)
        clean_names.append(c_name if c_name else "unnamed")
    combined_df.columns = clean_names

    # 2. 중복 컬럼 통합 (Merge duplicated columns)
    duplicated_cols = combined_df.columns[combined_df.columns.duplicated()].unique()
    for col_name in duplicated_cols:
        cols_to_merge = combined_df.loc[:, combined_df.columns == col_name]
        merged_values = cols_to_merge.ffill(axis=1).iloc[:, -1]
        combined_df = combined_df.loc[:, combined_df.columns != col_name]
        combined_df[col_name] = merged_values

    # 3. Object 타입 NaN 처리 및 중복 행 제거
    obj_cols = combined_df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        combined_df[col] = combined_df[col].fillna('').astype(str).replace(['nan', 'None', 'nan.0'], '')
    
    return combined_df.drop_duplicates()
