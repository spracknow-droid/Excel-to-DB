import pandas as pd
import re
import constants as const

def format_specific_columns(df):
    """코드 성격의 컬럼을 문자열로 변환 및 .0 제거 [3]"""
    for col in const.TARGET_STR_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'nan.0'], '')
            df[col] = df[col].apply(lambda x: x.split('.') if x.endswith('.0') else x).str.strip()
    return df

def clean_date_columns(df):
    """날짜 컬럼에서 시간 정보 제거 (YYYY-MM-DD) [3, 4]"""
    for col in const.TARGET_DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    return df

def finalize_combined_df(all_data):
    """데이터프레임 리스트를 하나로 합치고 컬럼명 정제 [1, 4]"""
    if not all_data: return None
    combined_df = pd.concat(all_data, ignore_index=True)

    # 컬럼명 특수문자 제거 및 언더바 치환 [4]
    clean_names = []
    for col in combined_df.columns:
        c_name = re.sub(r'[^a-zA-Z0-9가-힣]', '_', str(col)).strip('_')
        c_name = re.sub(r'_+', '_', c_name)
        clean_names.append(c_name if c_name else "unnamed")
    combined_df.columns = clean_names

    # 결측치 처리 및 중복 행 제거 [1]
    obj_cols = combined_df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        combined_df[col] = combined_df[col].fillna('').astype(str)
    
    return combined_df.drop_duplicates()
