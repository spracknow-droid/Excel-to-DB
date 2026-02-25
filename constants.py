### 자주 바뀌거나 관리해야 할 컬럼명, 매핑 규칙을 모아둔 파일 ###

# 1. 문자열로 강제 변환할 컬럼 리스트
TARGET_STR_COLS = ['매출처', '수금처', '납품처', '품목', '품목명', '품번']

# 2. 날짜 형식으로 정제할 컬럼 리스트
TARGET_DATE_COLS = ['계획년월', '매출일', '수금예정일', '출고일']

# 3. 판매계획 엑셀의 컬럼명 변경 매핑
PLAN_RENAME_MAP = {
    '품목코드': '품목',
    '판매수량': '수량',
    '품명': '품목명',
    '판매금액': '장부금액'
}

# 4. 최종 DB 및 엑셀 파일명
DB_FILENAME = "sales_integrated_final.db"
EXCEL_FILENAME = "sales_integrated_final.xlsx"
