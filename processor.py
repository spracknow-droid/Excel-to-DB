import pandas as pd
import sqlite3

def clean_data(df, target_type):
    """데이터 기본 정제: 공백 제거 및 날짜 형식 변환"""
    # 1. 모든 문자열 컬럼의 앞뒤 공백 제거
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    # 2. 날짜 관련 컬럼 처리 (날짜 형식으로 변환 후 시분초 제거)
    date_cols = ['매출일자', '계획일자', '납기일자', '출고일자']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            
    return df

def run_deduplication(conn, table_name, exclude_cols):
    """특정 컬럼을 제외하고 중복 데이터를 제거하는 핵심 함수"""
    try:
        # 현재 테이블의 전체 컬럼명 추출
        cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
        all_cols = [description[0] for description in cursor.description]
        
        # 제외 컬럼을 뺀 '기준 컬럼' 리스트 생성 (SQL 따옴표 처리)
        key_cols = [f'"{col}"' for col in all_cols if col not in exclude_cols]
        group_key = ", ".join(key_cols)
        
        # 기준 컬럼들이 동일한 행들 중 가장 먼저 들어온(MIN rowid) 행만 남기고 삭제
        query = f"""
            DELETE FROM {table_name} 
            WHERE rowid NOT IN (
                SELECT MIN(rowid) 
                FROM {table_name} 
                GROUP BY {group_key}
            )
        """
        conn.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error during deduplication: {e}")

def get_duplicates(conn, table_name, exclude_cols):
    """중복된 데이터가 무엇인지 보여주기 위한 조회 함수"""
    try:
        cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
        all_cols = [description[0] for description in cursor.description]
        
        key_cols = [f'"{col}"' for col in all_cols if col not in exclude_cols]
        group_key = ", ".join(key_cols)
        
        # 중복된 항목과 카운트 조회
        query = f"""
            SELECT *, COUNT(*) as '중복횟수'
            FROM {table_name}
            GROUP BY {group_key}
            HAVING COUNT(*) > 1
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error getting duplicates: {e}")
        return pd.DataFrame()
