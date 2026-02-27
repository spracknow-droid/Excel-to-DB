import pandas as pd

def clean_data(df, file_type):
    """
    엑셀 데이터를 업로드 직후 정제하는 함수
    - 숫자열로 오인될 수 있는 주요 컬럼을 문자열로 강제 변환
    - 모든 문자열의 앞뒤 공백 제거
    """
    # 1. 모든 문자열 데이터 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 파일 타입별 특정 컬럼 문자열(String) 강제 변환
    if file_type == "SLSSPN":  # 판매계획
        target_cols = ['매출처', '품목코드']
    elif file_type == "BILBIV":  # 매출리스트
        target_cols = ['매출처', '품목', '수금처', '납품처']
    else:
        target_cols = []

    for col in target_cols:
        if col in df.columns:
            # NaN은 빈 문자열로 바꾸거나 유지하며 문자열로 변환
            df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>'], '')
            
    return df
