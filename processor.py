import pandas as pd
import re
import constants as const

def format_specific_columns(df):
    """코드 성격의 컬럼을 문자열로 변환 및 .0 제거 [1]"""
    for col in const.TARGET_STR_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).replace(['nan', 'None', 'nan.0'], '')
            df[col] = df[col].apply(lambda x: x.split('.') if x.endswith('.0') else x).str.strip()
    return df

def clean_date_columns(df):
    """날짜 컬럼에서 시간 정보 제거 (YYYY-MM-DD) [2]"""
    for col in const.TARGET_DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    return df

def finalize_combined_df(all_data):
    """데이터프레임 리스트를 하나로 합치고 컬럼명 정제 [3, 4]"""
    if not all_data: return None
    combined_df = pd.concat(all_data, ignore_index=True)

    # 컬럼명 특수문자 제거
    clean_names = []
    for col in combined_df.columns:
        c_name = re.sub(r'[^a-zA-Z0-9가-힣]', '_', str(col)).strip('_')
        c_name = re.sub(r'_+', '_', c_name)
        clean_names.append(c_name if c_name else "unnamed")
    combined_df.columns = clean_names

    # 중복 제거
    obj_cols = combined_df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        combined_df[col] = combined_df[col].fillna('').astype(str)
    
    return combined_df.drop_duplicates()

def create_unified_total_df(plan_df, result_df):
    """의미가 다른 컬럼들을 구분하여 하나의 통합 테이블로 결합"""
    if plan_df is None and result_df is None: return None
    
    # 1. 공통 키 설정 (기준이 되는 정보)
    common_keys = ['매출처', '품목', '품목명'] 
    
    # 2. 각 데이터프레임의 컬럼명에 접두어 추가 (공통 키 제외)
    if plan_df is not None:
        plan_df = plan_df.rename(columns={c: f"계획_{c}" for c in plan_df.columns if c not in common_keys})
    
    if result_df is not None:
        result_df = result_df.rename(columns={c: f"실적_{c}" for c in result_df.columns if c not in common_keys})

    # 3. Outer Join을 통해 통합 테이블 생성
    if plan_df is not None and result_df is not None:
        total_df = pd.merge(plan_df, result_df, on=[k for k in common_keys if k in plan_df.columns and k in result_df.columns], how='outer')
    else:
        total_df = plan_df if plan_df is not None else result_df
        
    return total_df
