import pandas as pd
import re
import constants as const

def format_specific_columns(df):
    """코드 성격의 컬럼을 문자열로 변환 및 .0 제거 [2]"""
    for col in const.TARGET_STR_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'nan.0'], '')
            df[col] = df[col].apply(lambda x: x.split('.') if x.endswith('.0') else x).str.strip()
    return df

def clean_date_columns(df):
    """날짜 컬럼에서 시간 정보 제거 (YYYY-MM-DD) [2, 3]"""
    for col in const.TARGET_DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    return df

def finalize_combined_df(all_data):
    """통합된 데이터프레임의 컬럼명 정제 및 중복 제거 [4, 5]"""
    if not all_data: return None
    combined_df = pd.concat(all_data, ignore_index=True)

    # 1. 컬럼명 특수문자 제거 및 정제 [4]
    clean_names = []
    for col in combined_df.columns:
        c_name = re.sub(r'[^a-zA-Z0-9가-힣]', '_', str(col)).strip('_')
        c_name = re.sub(r'_+', '_', c_name)
        clean_names.append(c_name if c_name else "unnamed")
    combined_df.columns = clean_names

    # 2. 중복 컬럼 통합 [5]
    duplicated_cols = combined_df.columns[combined_df.columns.duplicated()].unique()
    for col_name in duplicated_cols:
        cols_to_merge = combined_df.loc[:, combined_df.columns == col_name]
        merged_values = cols_to_merge.ffill(axis=1).iloc[:, -1]
        combined_df = combined_df.loc[:, combined_df.columns != col_name]
        combined_df[col_name] = merged_values

    # 3. 중복 행 제거 [5]
    obj_cols = combined_df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        combined_df[col] = combined_df[col].fillna('').astype(str).replace(['nan', 'None', 'nan.0'], '')
    
    return combined_df.drop_duplicates()

def create_analysis_df(plan_df, result_df):
    """계획과 실적을 비교 분석하는 테이블 생성 (추가된 로직)"""
    if plan_df is None or result_df is None: return None

    # 기준년월 생성 (YYYY-MM)
    p_df = plan_df.copy()
    r_df = result_df.copy()
    
    if '계획년월' in p_df.columns: p_df['기준년월'] = p_df['계획년월'].str[:7]
    if '매출일' in r_df.columns: r_df['기준년월'] = r_df['매출일'].str[:7]

    # 계획 집계
    p_agg = p_df.groupby(['기준년월', '매출처', '품목'], as_index=False).agg({
        '수량': 'sum', '판매금액': 'sum'
    }).rename(columns={'수량': '계획수량', '판매금액': '계획금액'})

    # 실적 집계
    r_agg = r_df.groupby(['기준년월', '매출처', '품목'], as_index=False).agg({
        '수량': 'sum', '장부금액': 'sum'
    }).rename(columns={'수량': '실적수량', '장부금액': '실적금액'})

    # 병합 및 차이 계산
    analysis_df = pd.merge(p_agg, r_agg, on=['기준년월', '매출처', '품목'], how='outer').fillna(0)
    analysis_df['수량차이'] = analysis_df['실적수량'] - analysis_df['계획수량']
    analysis_df['금액차이'] = analysis_df['실적금액'] - analysis_df['계획금액']
    return analysis_df
