import pandas as pd

def clean_data(df, file_type):
    """
    데이터 로드 직후 '시분초'를 박멸하고 날짜 포맷을 통일하는 함수
    """
    # 1. 문자열 앞뒤 공백 제거 (기본)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 날짜 관련 컬럼 처리 (시분초 삭제)
    # 컬럼명에 '일자', '날짜', '년월', 'Date'가 들어가면 무조건 수술
    date_keywords = ['일자', '날짜', '년월', 'Date']
    
    for col in df.columns:
        if any(key in col for key in date_keywords):
            try:
                # 데이터를 날짜형으로 변환 (에러는 무시)
                temp_date = pd.to_datetime(df[col], errors='coerce')
                # 시분초 다 버리고 YYYY-MM-DD 문자열로 강제 고정
                df[col] = temp_date.dt.strftime('%Y-%m-%d')
                
                # 변환 실패해서 NaT 된 것들은 원래 값이라도 유지
                df[col] = df[col].fillna(df[col])
            except:
                pass

    return df
