import sqlite3

def create_sales_views(conn):
    cursor = conn.cursor()

    # 기존 View가 있다면 삭제
    cursor.execute("DROP VIEW IF EXISTS view_plan_vs_actual")

    # 신규 분석 View 생성
    cursor.execute("""
        CREATE VIEW view_plan_vs_actual AS
        WITH MonthlyPlan AS (
            SELECT 
                strftime('%Y-%m', 계획년월) AS standard_month,
                매출처명,
                품명,
                SUM(판매수량) AS plan_qty,
                SUM(판매금액) AS plan_amount_krw  -- 판매계획의 원화금액
            FROM sales_plan_data
            GROUP BY 1, 2, 3
        ),
        MonthlyActual AS (
            SELECT 
                strftime('%Y-%m', 매출일) AS standard_month,
                매출처명,
                품목명 AS 품명,
                SUM(수량) AS actual_qty,
                SUM(장부금액) AS actual_amount_krw  -- 판매실적의 원화 환산액(장부금액)
            FROM sales_actual_data
            GROUP BY 1, 2, 3
        ),
        AllKeys AS (
            SELECT standard_month, 매출처명, 품명 FROM MonthlyPlan
            UNION
            SELECT standard_month, 매출처명, 품명 FROM MonthlyActual
        )
        SELECT 
            k.standard_month AS 분석월,
            k.매출처명,
            k.품명,
            -- 수량 비교
            IFNULL(p.plan_qty, 0) AS 계획수량,
            IFNULL(a.actual_qty, 0) AS 실적수량,
            IFNULL(a.actual_qty, 0) - IFNULL(p.plan_qty, 0) AS 수량차이,
            
            -- 금액 비교 (원화 기준 통일)
            IFNULL(p.plan_amount_krw, 0) AS 계획금액_원화,
            IFNULL(a.actual_amount_krw, 0) AS 실적금액_원화,
            IFNULL(a.actual_amount_krw, 0) - IFNULL(p.plan_amount_krw, 0) AS 금액차이_원화,
            
            -- 달성률 계산
            CASE 
                WHEN IFNULL(p.plan_amount_krw, 0) = 0 THEN 0 
                ELSE ROUND(CAST(IFNULL(a.actual_amount_krw, 0) AS FLOAT) / p.plan_amount_krw * 100, 1) 
            END AS 매출달성률
        FROM AllKeys k
        LEFT JOIN MonthlyPlan p ON k.standard_month = p.standard_month AND k.매출처명 = p.매출처명 AND k.품명 = p.품명
        LEFT JOIN MonthlyActual a ON k.standard_month = a.standard_month AND k.매출처명 = a.매출처명 AND k.품명 = a.품명
    """)

    conn.commit()
