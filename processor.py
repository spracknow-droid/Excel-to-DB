import pandas as pd

def clean_data(df, file_type):
    """
    엑셀 데이터를 읽은 직후 정제하는 함수
    """
    # 1. 모든 데이터의 앞뒤 공백 제거 (문자열인 경우만)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 날짜 형식 컬럼 변환 (YYYY-MM-DD)
    for col in df.columns:
        if any(keyword in col for keyword in ['일자', '날짜', 'Date']):
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            except:
                pass 
            
    return df
