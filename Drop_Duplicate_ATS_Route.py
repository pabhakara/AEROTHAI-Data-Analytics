import psycopg2

table_name = 'ats_route_length'

conn = psycopg2.connect(user = "postgres",
                        password = "password",
                        host = "127.0.0.1",
                        port = "5432",
                        database = "airac_2021_08")

with conn:
    cur = conn.cursor()

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "SELECT *, ST_Length(ST_Transform(geom, 3857)) / 1852 as length " \
                + 'into ' + table_name + ' from ' \
                + 'ats_route;' \
 \
        #print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    sql_query = "SELECT ats_route_id, sub_route_id, length from ats_route_temp order by ats_route_id,length;"

    print(sql_query)

    cur.execute(sql_query)

    results = cur.fetchall()

    # initialize parameters
    ats_route_id = []
    sub_route_id = []
    length = []

    # populate the parameters with the query results, row by row
    for row in results:
        ats_route_id.append(row[0])
        sub_route_id.append(row[1])
        length.append(row[2])

    print(sql_query)

    cur.execute(sql_query)

    print("Number of rows: " + str(len(ats_route_id)))
    num_of_ids = len(ats_route_id)

    k = 0

    print(ats_route_id[k])
    print(ats_route_id[k + 1])

    print(sub_route_id[k])
    print(sub_route_id[k + 1])

    print(length[k])
    print(length[k+1])

    while k < num_of_ids - 1:
        if abs(length[k] - length[k+1]) < .01:
            sql_query = 'DELETE FROM ats_route_temp ' + \
                        'WHERE ats_route_id = \'' + ats_route_id[k+1] + \
                        '\' and sub_route_id = \'' + sub_route_id[k+1] + '\';'
            print(sql_query)
            cur.execute(sql_query)
            k = k + 1
        k = k + 1