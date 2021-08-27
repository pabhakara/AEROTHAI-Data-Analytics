import psycopg2.extras

table_name = 'mora_grid'

conn = psycopg2.connect(user = "postgres",
                                password = "password",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "current_airac")

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

    print(cur.rowcount)

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
            print(temp['mora' + m_txt])

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
            print(sql_text_2)

            conn.commit()
            print(str("{:.3f}".format((k / num_of_rows) * 100, 2)) + "% Completed")

            m = m + 1

        print(k)

        k = k + 1

    # mora01_txt = str(temp['mora01'])
    # mora02_txt = str(temp['mora02'])
    # mora03_txt = str(temp['mora03'])
    # mora04_txt = str(temp['mora04'])
    # mora05_txt = str(temp['mora05'])
    # mora06_txt = str(temp['mora06'])
    # mora07_txt = str(temp['mora07'])
    # mora08_txt = str(temp['mora08'])
    # mora09_txt = str(temp['mora09'])
    # mora10_txt = str(temp['mora10'])
    # mora11_txt = str(temp['mora11'])
    # mora12_txt = str(temp['mora12'])
    # mora13_txt = str(temp['mora13'])
    # mora14_txt = str(temp['mora14'])
    # mora15_txt = str(temp['mora15'])
    # mora16_txt = str(temp['mora16'])
    # mora17_txt = str(temp['mora17'])
    # mora18_txt = str(temp['mora18'])
    # mora19_txt = str(temp['mora19'])
    # mora20_txt = str(temp['mora20'])
    # mora21_txt = str(temp['mora21'])
    # mora22_txt = str(temp['mora22'])
    # mora23_txt = str(temp['mora23'])
    # mora24_txt = str(temp['mora24'])
    # mora25_txt = str(temp['mora25'])
    # mora26_txt = str(temp['mora26'])
    # mora27_txt = str(temp['mora27'])
    # mora28_txt = str(temp['mora28'])
    # mora29_txt = str(temp['mora29'])
    # mora30_txt = str(temp['mora30'])
    #
    # mora01_int = int(temp['mora01'])
    # mora02_txt = int(temp['mora02'])
    # mora03_txt = int(temp['mora03'])
    # mora04_txt = int(temp['mora04'])
    # mora05_txt = int(temp['mora05'])
    # mora06_txt = int(temp['mora06'])
    # mora07_txt = int(temp['mora07'])
    # mora08_txt = int(temp['mora08'])
    # mora09_txt = int(temp['mora09'])
    # mora10_txt = int(temp['mora10'])
    # mora11_txt = int(temp['mora11'])
    # mora12_txt = int(temp['mora12'])
    # mora13_txt = int(temp['mora13'])
    # mora14_txt = int(temp['mora14'])
    # mora15_txt = int(temp['mora15'])
    # mora16_txt = int(temp['mora16'])
    # mora17_txt = int(temp['mora17'])
    # mora18_txt = int(temp['mora18'])
    # mora19_txt = int(temp['mora19'])
    # mora20_txt = int(temp['mora20'])
    # mora21_txt = int(temp['mora21'])
    # mora22_txt = int(temp['mora22'])
    # mora23_txt = int(temp['mora23'])
    # mora24_txt = int(temp['mora24'])
    # mora25_txt = int(temp['mora25'])
    # mora26_txt = int(temp['mora26'])
    # mora27_txt = int(temp['mora27'])
    # mora28_txt = int(temp['mora28'])
    # mora29_txt = int(temp['mora29'])
    # mora30_txt = int(temp['mora30'])



