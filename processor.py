import pandas as pd

def clean_data(df, file_type):
    """
    엑셀 데이터를 업로드 직후 정제하는 함수
    """
    # 1. 모든 문자열 데이터 앞뒤 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 날짜 형식 컬럼 변환 (YYYY-MM-DD)
    # 엑셀에서 날짜로 인식되는 컬럼들을 찾아 변환합니다.
    for col in df.columns:
        if '일자' in col or '날짜' in col or 'Date' in col:
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            except:
                pass # 날짜 형식이 아니면 건너뜀

    # 3. 특정 컬럼 문자열(String) 강제 변환
    if file_type == "SLSSPN":  # 판매계획
        target_cols = ['매출처', '품목코드']
    elif file_type == "BILBIV":  # 매출리스트
        target_cols = ['매출처', '품목', '수금처', '납품처']
    else:
        target_cols = []

    for col in target_cols:
        if col in df.columns:
            # NaN 처리 및 문자열 변환
            df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>', 'nan.0'], '')
            
    return df
