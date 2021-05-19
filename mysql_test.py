import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='flight',
                                         user='root',
                                         password='password')
    if connection.is_connected():
        mysql_query = "SELECT a.flight_id,b.app_time,a.dep,a.dest,a.actype,a.ftype,a.op_type,a.etd,a.atd,a.eta,a.ata,a.flevel," + \
        "b.latitude,b.longitude,b.actual_flight_level,b.cdm," + \
        "b.sector,a.callsign,a.route,a.pbn_type,a.comnav," + \
        "a.sidstar,a.runway,a.frule,a.reg,a.entry_flevel,a.maintain_flevel,a.exit_flevel " + \
        "from 2019_12_radar a, target_2019_12 b " + \
        "where (a.flight_id = b.flight_id) and a.flight_id = 1 " + \
        "and (day(a.entry_time) < 3) and (day(a.entry_time) >=1 ) " + \
        "order by a.flight_id,b.app_time"
        print(mysql_query)

        cursor = connection.cursor(dictionary=True)
        cursor.execute(mysql_query)
        record = cursor.fetchall()
        num_of_records = len(record)
        print(num_of_records)

        temp_1 = record[0]
        print(temp_1)

        callsign = str(temp_1['callsign'])
        app_time = str(temp_1['app_time'])
        etd =str(temp_1['etd'])
        atd = str(temp_1['atd'])
        eta = str(temp_1['eta'])
        ata = str(temp_1['ata'])
        dep = str(temp_1['dep'])
        dest = str(temp_1['dest'])
        reg = str(temp_1['reg'])

        actype = str(temp_1['actype'])
        runway = str(temp_1['runway'])
        sidstar = str(temp_1['sidstar'])
        pbn_type = str(temp_1['pbn_type'])
        op_type = str(temp_1['op_type'])
        frule = str(temp_1['frule'])
        comnav = str(temp_1['comnav'])
        route = str(temp_1['route'])

        flevel = str(temp_1['flevel'])
        entry_flevel = str(temp_1['entry_flevel'])
        maintain_flevel = str(temp_1['maintain_flevel'])
        exit_flevel = str(temp_1['exit_flevel'])

        table_name = "ABC"

        postgres_sql_text = "INSERT INTO \"" + table_name + "\" (\"callsign\"," + \
                            "\"start_time\",\"etd\",\"atd\",\"eta\",\"ata\"," + \
                            "\"dep\",\"dest\",\"reg\",\"actype\",\"runway\"," + \
                            "\"sidstar\",\"pbn_type\"," + \
                            "\"op_type\",\"frule\",\"comnav\",\"route\"," + \
                            "\"flevel\",\"entry_flevel\",\"maintain_flevel\",\"exit_flevel\"," + \
                            "\"flight_id\",\"geom\",\"end_time\")"

        postgres_sql_text = postgres_sql_text + " VALUES('" + callsign + "','" \
                + app_time + "','" \
                + etd + "','" \
                + atd + "','" \
                + eta + "','" \
                + ata + "','" \
                + dep + "','" \
                + dest + "','" \
                + reg + "','" \
                + actype + "','" \
                + runway + "','" \
                + sidstar + "','" \
                + pbn_type + "','" \
                + op_type + "','" \
                + frule + "','" \
                + comnav + "','" \
                + route + "','" \
                + flevel + "','" \
                + entry_flevel + "','" \
                + maintain_flevel + "','" \
                + exit_flevel + "')"

        print(postgres_sql_text)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")

