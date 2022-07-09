import psycopg2.extras

from dbname_and_paths import db_name,airac,schema_name

table_name = 'mora_grid'

conn = psycopg2.connect(
    user='postgres', password='password',
    host='127.0.0.1', port='5432',
    database=db_name,
    options="-c search_path=dbo," + schema_name
)

with conn:
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sql_query = "DROP TABLE IF EXISTS " + table_name + ";"

    cur.execute(sql_query)
    conn.commit()

    sql_query = "CREATE TABLE " + table_name \
                + '(starting_latitude integer, ' \
                + 'starting_longitude integer,' \
                + 'ending_latitude integer,' \
                + 'ending_longitude integer,' \
                + 'value_txt varchar,' \
                + 'value_int integer,' \
                + 'geom geometry) ' \
                + 'WITH (OIDS=FALSE);' \
                + 'ALTER TABLE "' + table_name + '"'  \
                + 'OWNER TO postgres;'

    cur.execute(sql_query)
    conn.commit()

    sql_query = "SELECT * FROM public.tbl_grid_mora" \
                " order by starting_latitude,starting_longitude ASC;"

    print(sql_query)

    cur.execute(sql_query)

    num_of_rows = cur.rowcount

    results = cur.fetchall()

    print("Total rows are:  ", len(results))

    k = 0

    while k < num_of_rows:

        temp = results[k]

        starting_latitude = temp[0]
        starting_longitude = temp[1]

        m = 1

        if m < 10:
            m_txt = '0' + str(m)
        else:
            m_txt = str(m)

        while  m < 31:
            if m < 10:
                m_txt = '0' + str(m)
            else:
                m_txt = str(m)
            #print(temp['mora' + m_txt])

            if temp['mora' + m_txt] == 'UNK':
                mora_value = - 1
            else:
                mora_value = int(temp['mora' + m_txt])

            starting_latitude_int = int(starting_latitude)
            ending_latitude_int = starting_latitude_int + 1

            starting_longitude_int = int(starting_longitude) + m - 1
            ending_longitude_int = starting_latitude_int + 1

            sql_text_2 = "INSERT INTO " + table_name + \
                         "(starting_latitude," + \
                         "starting_longitude," + \
                         "ending_latitude," + \
                         "ending_longitude," + \
                         "value_txt," + \
                         "value_int," + \
                         "geom) " + \
                         "VALUES(" + \
                         str(starting_latitude_int) + "," + \
                         str(starting_longitude_int) + "," + \
                         str(starting_latitude_int + 1) + "," + \
                         str(starting_longitude_int + 1) + ",'" + \
                         str(temp['mora' + m_txt]) + "'," + \
                         str(mora_value) + "," + \
                         "ST_GeomFromText('POLYGON((" + \
                         str(starting_longitude_int) + " " +  str(starting_latitude_int) + "," + \
                         str(starting_longitude_int) + " " +  str(starting_latitude_int + 1) + "," + \
                         str(starting_longitude_int + 1) + " " +  str(starting_latitude_int + 1) + "," + \
                         str(starting_longitude_int + 1) + " " +  str(starting_latitude_int) + "," + \
                         str(starting_longitude_int) + " " +  str(starting_latitude_int) + "))',4326));"
            cur.execute(sql_text_2)

            conn.commit()
            print("Grid MORA: " + str("{:.3f}".format((k / num_of_rows) * 100, 2)) + "% Completed")

            m = m + 1

        k = k + 1
