import psycopg2

try:
    # Setup Postgres DB connection
    conn = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    cur = conn.cursor()

    filepath = 'TerminalLegs.sql'
    with open(filepath) as fp:
        for cnt, line in enumerate(fp):
            cur.execute(line)
            conn.commit()
            print("Line {}: {}".format(cnt, line))

except (Exception, psycopg2.DatabaseError) as error:
    print(error)

finally:
    if conn is not None:
        conn.close()
