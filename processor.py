import pandas as pd

def clean_data(df, file_type):
    """
    사용자가 지정한 4대 핵심 날짜 컬럼의 시분초를 완벽하게 제거
    지정 컬럼: '매출일', '수금예정일', '출고일', '계획년월'
    """
    # 1. 모든 데이터의 앞뒤 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 2. 사용자 지정 4대 날짜 컬럼 정밀 타격
    # 어떤 파일 타입이든 상관없이 아래 4개 컬럼명이 존재하면 시분초를 삭제함
    target_date_cols = ['매출일', '수금예정일', '출고일', '계획년월']

    for col in df.columns:
        if col in target_date_cols:
            try:
                # 00:00:00 시간을 완전히 잘라내고 YYYY-MM-DD 포맷 문자열로 고정
                # errors='coerce'로 변환 불가능한 값은 유지하면서 날짜만 깔끔하게 정리
                temp_date = pd.to_datetime(df[col], errors='coerce')
                df[col] = temp_date.dt.strftime('%Y-%m-%d')
                
                # 변환 실패(NaT) 데이터는 원본 그대로 두어 데이터 소실 방지
                df[col] = df[col].fillna(df[col])
            except:
                pass

    return df
