import pandas as pd

def clean_data(df, file_type):
    """
    엑셀 데이터를 읽은 직후 가장 먼저 실행되는 정제 함수
    """
    # 1. 모든 데이터의 앞뒤 공백 제거 (문자열인 경우)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 날짜 형식 컬럼 변환 (YYYY-MM-DD)
    # 컬럼명에 '일자', '날짜', 'Date'가 포함된 경우 처리
    for col in df.columns:
        if any(keyword in col for keyword in ['일자', '날짜', 'Date']):
            try:
                # 이미 문자열인 데이터를 날짜형으로 바꿨다가 다시 지정된 포맷으로 변경
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            except:
                pass 

    # 3. 특정 컬럼 문자열(String) 강제 변환 및 결측치 처리
    # (이미 read_excel에서 문자열로 읽었으므로 공백/NaN 처리 위주)
    if file_type == "SLSSPN":
        target_cols = ['매출처', '품목코드']
    elif file_type == "BILBIV":
        target_cols = ['매출처', '품목', '수금처', '납품처']
    else:
        target_cols = []

    for col in target_cols:
        if col in df.columns:
            # 문자열화 및 불필요한 표기(nan 등) 제거
            df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>', 'nan.0'], '')
            
    return df
