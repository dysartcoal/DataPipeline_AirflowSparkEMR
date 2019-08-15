class SqlQueries:
    test_sql = """SELECT visatype, count(visatype) as visatype_count
                FROM i94
                WHERE i94mode=1
                GROUP BY visatype"""
