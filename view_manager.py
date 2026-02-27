import sqlite3

def create_sales_views(conn):
    cursor = conn.cursor()

    # 1. 판매계획 전처리 (매출리스트 컬럼명에 맞춤)
    cursor.execute("DROP VIEW IF EXISTS view_cleaned_plan")
    cursor.execute("""
        CREATE VIEW view_cleaned_plan AS
        SELECT 
            strftime('%Y-%m', 계획년월) AS 기준월,
            매출처명,
            품명 AS 품목명,      -- 매출리스트의 '품목명'에 맞춤   
            판매수량 AS 수량,      -- 매출리스트의 '수량'에 맞춤
            판매금액 AS 장부금액    -- 매출리스트의 '장부금액' 자리에 계획(원화) 배치
        FROM sales_plan_data
    """)

    # 2. 매출리스트 전처리 (필요한 컬럼만 선별)
    cursor.execute("DROP VIEW IF EXISTS view_cleaned_actual")
    cursor.execute("""
        CREATE VIEW view_cleaned_actual AS
        SELECT 
            strftime('%Y-%m', 매출일) AS 기준월,
            매출처명,
            품목명
            수량,
            장부금액              -- 실적의 원화금액인 장부금액 사용
        FROM sales_actual_data
    """)

    conn.commit()
