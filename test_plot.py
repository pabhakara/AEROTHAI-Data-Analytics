import numpy as np
import matplotlib.pyplot as plt
import psycopg2

N = 50
x = np.random.rand(N)
y = np.random.rand(N)

try:
    conn = psycopg2.connect(user = "postgres",
                                  password = "password",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM airport
        WHERE ident like 'VT%';
    """)
    row = cur.fetchone()
    while row is not None:
        plt.scatter(float(row[4]), float(row[3]))
        row = cur.fetchone()
    plt.show()
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
