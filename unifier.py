import pandas as pd

def create_unified_total_df(plan_df, result_df):
    """의미가 다른 컬럼들을 구분하여 하나의 통합 테이블로 결합 [1, 2]"""
    if plan_df is None and result_df is None: 
        return None
    
    # 1. 공통 키 설정 (기준이 되는 정보) [1]
    common_keys = ['매출처', '품목', '품목명']
    
    # 2. 각 데이터프레임의 컬럼명에 접두어 추가 (공통 키 제외) [1]
    if plan_df is not None:
        plan_df = plan_df.rename(columns={c: f"계획_{c}" for c in plan_df.columns if c not in common_keys})
    
    if result_df is not None:
        result_df = result_df.rename(columns={c: f"실적_{c}" for c in result_df.columns if c not in common_keys})

    # 3. Outer Join을 통해 통합 테이블 생성 [2]
    if plan_df is not None and result_df is not None:
        # 두 데이터에 공통으로 존재하는 키만 추출하여 병합 기준으로 사용 [2]
        merge_keys = [k for k in common_keys if k in plan_df.columns and k in result_df.columns]
        total_df = pd.merge(plan_df, result_df, on=merge_keys, how='outer')
    else:
        total_df = plan_df if plan_df is not None else result_df
        
    return total_df
