import pandas as pd

def clean_data(df, file_type):
    """
    엑셀 데이터를 읽은 직후 정제하는 함수
    """
    # 1. 모든 문자열 데이터 앞뒤 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 날짜 형식 컬럼 정밀 변환 (시간 제거: YYYY-MM-DD)
    for col in df.columns:
        if any(keyword in col for keyword in ['일자', '날짜', 'Date']):
            try:
                # errors='coerce'를 사용하여 변환 불가 데이터는 NaT 처리
                # dt.date를 호출하여 '00:00:00' 같은 시간 정보를 원천적으로 날려버림
                temp_date = pd.to_datetime(df[col], errors='coerce')
                df[col] = temp_date.dt.strftime('%Y-%m-%d')
                
                # 만약 변환 과정에서 NaT가 된 것은 기존 값 유지 (데이터 손실 방지)
                df[col] = df[col].fillna(df[col]) 
            except:
                pass 
            
    return df
