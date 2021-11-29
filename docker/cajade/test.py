import psycopg2


if __name__=='__main__':
    conn = psycopg2.connect(f"host=10.5.0.3 dbname=nba01 user=cajade password=reproduce port=5432")
    cur = conn.cursor()
    cur.execute("select * from team")
    print(cur.fetchall())

