import psycopg2

table_name = 'ats_route_segments_temp_2'

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

    EPGS = 32647 # UTM 47N

    EPSG = 32632 # UTM 32N

    EPGS = 32732 # UTM 32S

    sql_query = "SELECT *, ST_Length(ST_Transform(geom, 32647)) / 1852 as length " \
                + 'into ' + table_name + ' from ' \
                + 'ats_route_segments;' \

    #print(sql_query)

    cur.execute(sql_query)
    conn.commit()

    sql_query = "SELECT ats_route_id, sub_route_id, length, wp_start, wp_end from " \
            + table_name + " order by ats_route_id,length;"

    print(sql_query)

    cur.execute(sql_query)

    results = cur.fetchall()

    # initialize parameters
    ats_route_id = []
    sub_route_id = []
    length = []
    wp_start = []
    wp_end = []

    # populate the parameters with the query results, row by row
    for row in results:
        ats_route_id.append(row[0])
        sub_route_id.append(row[1])
        length.append(row[2])
        wp_start.append(row[3])
        wp_end.append(row[4])

    print(sql_query)

    cur.execute(sql_query)

    print("Number of rows: " + str(len(ats_route_id)))
    num_of_ids = len(ats_route_id)

    k = 0

    # delete duplicate segments row by row
    while k < num_of_ids - 1:
        if (abs(length[k] - length[k+1]) < .01) and \
                (wp_start[k] == wp_end[k+1]) and \
                (wp_end[k] == wp_start[k+1]):
            sql_query = 'DELETE FROM ' + table_name + \
                        ' WHERE ats_route_id = \'' + ats_route_id[k+1] + \
                        '\' and sub_route_id = \'' + sub_route_id[k+1] + \
                        '\' and wp_start = \'' + wp_start[k + 1] + \
                        '\' and wp_end = \'' + wp_end[k+1] + '\';'
            print(sql_query)
            cur.execute(sql_query)
            k = k + 1
        k = k + 1