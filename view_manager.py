import sqlite3

def create_sales_views(conn):
    """
    판매 계획(Plan)과 실적(Actual)을 분석하기 위한 고도화된 View들을 생성합니다.
    """
    cursor = conn.cursor()

    # 1. 월별 계획 대비 실적 통합 View (핵심)
    # 계획년월(YYYY-MM-DD)과 매출일(YYYY-MM-DD)을 'YYYY-MM' 형식으로 맞춰서 조인합니다.
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS view_plan_vs_actual AS
        WITH MonthlyPlan AS (
            SELECT 
                strftime('%Y-%m', 계획년월) AS standard_month,
                매출처명,
                품명,
                SUM(판매수량) AS plan_qty,
                SUM(판매금액) AS plan_amount
            FROM sales_plan_data
            GROUP BY 1, 2, 3
        ),
        MonthlyActual AS (
            SELECT 
                strftime('%Y-%m', 매출일) AS standard_month,
                매출처명,
                품목명 AS 품명,
                SUM(수량) AS actual_qty,
                SUM(판매금액) AS actual_amount
            FROM sales_actual_data
            GROUP BY 1, 2, 3
        )
        SELECT 
            COALESCE(p.standard_month, a.standard_month) AS 분석월,
            COALESCE(p.매출처명, a.매출처명) AS 매출처명,
            COALESCE(p.품명, a.품명) AS 품명,
            IFNULL(p.plan_qty, 0) AS 계획수량,
            IFNULL(a.actual_qty, 0) AS 실적수량,
            IFNULL(a.actual_qty, 0) - IFNULL(p.plan_qty, 0) AS 수량차이,
            CASE 
                WHEN IFNULL(p.plan_qty, 0) = 0 THEN 0 
                ELSE ROUND(CAST(IFNULL(a.actual_qty, 0) AS FLOAT) / p.plan_qty * 100, 1) 
            END AS 수량달성률,
            IFNULL(p.plan_amount, 0) AS 계획금액,
            IFNULL(a.actual_amount, 0) AS 실적금액,
            IFNULL(a.actual_amount, 0) - IFNULL(p.plan_amount, 0) AS 금액차이
        FROM MonthlyPlan p
        FULL OUTER JOIN MonthlyActual a 
            ON p.standard_month = a.standard_month 
            AND p.매출처명 = a.매출처명 
            AND p.품명 = a.품명
    """)

    # 2. 매출처별 실적 순위 View (Top Customers)
    cursor.execute("""
        CREATE VIEW IF NOT EXISTS view_top_customers AS
        SELECT 
            매출처명,
            SUM(수량) AS 총판매수량,
            SUM(판매금액) AS 총판매금액,
            COUNT(DISTINCT 품목명) AS 취급품목수
        FROM sales_actual_data
        GROUP BY 매출처명
        ORDER BY 총판매금액 DESC
    """)

    conn.commit()
    print("✅ 분석용 View 생성이 완료되었습니다.")
