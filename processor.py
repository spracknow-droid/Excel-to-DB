def run_deduplication(conn, table_name, exclude_cols):
    """지정된 테이블에서 특정 컬럼을 제외한 기준값으로 중복 제거"""
    try:
        all_cols = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 0", conn).columns.tolist()
        key_cols = [f'"{col}"' for col in all_cols if col not in exclude_cols]
        group_key = ", ".join(key_cols)
        
        query = f"""
            DELETE FROM {table_name} 
            WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM {table_name} GROUP BY {group_key}
            )
        """
        conn.execute(query)
        conn.commit()
    except:
        pass

def get_duplicates(conn, table_name, exclude_cols):
    """중복된 행들만 추출하여 반환 (시각화용)"""
    try:
        all_cols = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 0", conn).columns.tolist()
        key_cols = [f'"{col}"' for col in all_cols if col not in exclude_cols]
        group_key = ", ".join(key_cols)
        
        query = f"""
            SELECT *, COUNT(*) as '중복횟수'
            FROM {table_name}
            GROUP BY {group_key}
            HAVING COUNT(*) > 1
        """
        return pd.read_sql(query, conn)
    except:
        return pd.DataFrame()
