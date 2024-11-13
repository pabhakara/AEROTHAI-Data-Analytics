import csv
import datetime as dt
import time
from subprocess import PIPE, Popen

import pandas as pd

import psycopg2.extras

import psycopg2


def None_to_blank(x):
    if x == None:
        y = ''
    else:
        y = x
    return y

conn_postgres = psycopg2.connect(user = "postgres",
                                        password = "password",
                                        host = "127.0.0.1",
                                        port = "5432",
                                        database = "temp",
                                        options="-c search_path=dbo,public")

with open('approach_leg.csv', 'w', newline='') as csvfile:
    approach_leg = csv.writer(csvfile, delimiter=',')
    
    with conn_postgres:
        cursor_postgres = conn_postgres.cursor(cursor_factory=psycopg2.extras.DictCursor)
        postgres_sql_text = (f"SELECT airport_identifier,procedure_identifier,transition_identifier,seqno,"
                             f" waypoint_identifier,altitude_description,altitude1,altitude2,speed_limit "
                             f" FROM airac_current.stars_wp"
                             f" WHERE airport_identifier LIKE 'VT%' AND waypoint_icao_code LIKe 'VT'"
                             f" AND NOT ((waypoint_description_code LIKE '%EE%' OR seqno = 10) AND route_type = '5')  "
                             f" ORDER BY airport_identifier, procedure_identifier,route_type,transition_identifier,seqno;")

        print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)
        star_legs = cursor_postgres.fetchall()

        num_of_star_legs = len(star_legs)

        print(star_legs)
        print(num_of_star_legs)

        postgres_sql_text = (f"SELECT airport_identifier, procedure_identifier,route_type,transition_identifier,COUNT(*)"
                             f" FROM airac_current.stars_wp"
                             f" WHERE airport_identifier LIKE 'VT%' AND waypoint_icao_code LIKe 'VT' AND transition_identifier LIKE '%'  "
                             f" AND NOT ((waypoint_description_code LIKE '%EE%' OR seqno = 10) AND route_type = '5') "
                             f" GROUP BY airport_identifier, procedure_identifier,route_type,transition_identifier"
                             f" ORDER BY COUNT ASC;")
        print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)
        star_transitions = cursor_postgres.fetchall()

        num_of_star_transitions = len(star_transitions)

        print(star_transitions)
        print(num_of_star_transitions)

        postgres_sql_text = (
            f"SELECT airport_identifier, procedure_identifier,transition_identifier"
            f" FROM airac_current.stars_wp"
            f" WHERE airport_identifier LIKE 'VT%' AND waypoint_icao_code LIKe 'VT' AND route_type = '5'"
            f" AND transition_identifier LIKE '%RW'"
            f" GROUP BY airport_identifier, procedure_identifier,transition_identifier")
        print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)
        star_ident_runway_mapping = cursor_postgres.fetchall()

        for k in range(num_of_star_transitions):

            print(star_transitions[k])
            star_transition = star_transitions[k]

            airport_identifier = star_transition[0]
            procedure_identifier = star_transition[1]
            transition_identifier = star_transition[3]
            num_of_wps_in_star_transitions = star_transition[4]

            print(airport_identifier)
            print(procedure_identifier)
            print(transition_identifier)
            print(num_of_wps_in_star_transitions)

            postgres_sql_text = (f"SELECT runway "
                                 f" FROM temp.stars_runway_mapping"
                                 f" WHERE airport_identifier LIKE '{airport_identifier}'"
                                 f" AND procedure_identifier LIKE '{procedure_identifier}'")

            cursor_postgres.execute(postgres_sql_text)
            record = cursor_postgres.fetchall()
            runway_identifier_temp = record[0][0]
            #print(runway_identifier)

            if num_of_wps_in_star_transitions >= 2:

                postgres_sql_text = (f"SELECT airport_identifier,procedure_identifier,transition_identifier,seqno,"
                                     f" waypoint_identifier,altitude_description,altitude1,altitude2,speed_limit "
                                     f" FROM airac_current.stars_wp"
                                     f" WHERE airport_identifier LIKE '{airport_identifier}' "
                                     f" AND procedure_identifier LIKE '{procedure_identifier}' "
                                     f" AND transition_identifier LIKE '{transition_identifier}' "
                                     f" AND waypoint_icao_code LIKE 'VT'"
                                     f" AND NOT ((waypoint_description_code LIKE '%EE%' OR seqno = 10) AND route_type = '5')  "
                                     f" ORDER BY airport_identifier, procedure_identifier,route_type,transition_identifier,seqno;")

                #print(postgres_sql_text)

                cursor_postgres.execute(postgres_sql_text)
                star_legs = cursor_postgres.fetchall()

                if "B" in runway_identifier_temp:
                    runway_identifiers = [str.replace(runway_identifier_temp,"B","L")]
                    runway_identifiers.append(str.replace(runway_identifier_temp,"B","R"))
                else:
                    runway_identifiers = [runway_identifier_temp]

                print(runway_identifiers)

                for runway_identifier in runway_identifiers:

                    m = 0
                
                    approach_leg_header = f"{airport_identifier}.{runway_identifier}.{procedure_identifier}.{transition_identifier};true;;;;;;;;;"

                    if "RW" in transition_identifier:
                        next_approach_leg =  f"{airport_identifier}.{runway_identifier}.FINAL"
                    else:
                        next_approach_leg = f"{airport_identifier}.{runway_identifier}.{procedure_identifier}.RW{runway_identifier[0:2]}B"

                    approach_leg_element_list = []

                    approach_leg_element_list.append(f"{approach_leg_header}{next_approach_leg}")

                    approach_leg_element_list.extend(['None','','','','','','None','','','','','','','FALSE','BeginToEnd',''])

                    no1_waypoint_identifier = star_legs[0][4]

                    no1_altitude_description = None_to_blank(star_legs[0][5])
                    no1_altitude1 = None_to_blank(star_legs[0][6])
                    no1_altitude2 = None_to_blank(star_legs[0][7])

                    no2_altitude_description = None_to_blank(star_legs[1][5])
                    no2_altitude1 = None_to_blank(star_legs[1][6])
                    no2_altitude2 = None_to_blank(star_legs[1][7])

                    match no1_altitude_description:
                        case 'B':
                            no1_altitude_restriction = f"{no1_altitude2}ft|{no1_altitude1}ft"
                            typical_altitude = f"{no1_altitude2}ft"
                        case '-':
                            match no2_altitude_description:
                                case 'B':
                                    no1_altitude_restriction = f"{no2_altitude2}ft|{no2_altitude1 + 2000}ft"
                                    typical_altitude = f"{no2_altitude2}ft"
                                case '+':
                                    no1_altitude_restriction = f"{no2_altitude1}ft|{no1_altitude1}ft"
                                    typical_altitude = f"{no2_altitude1}ft"
                                case _:
                                    no1_altitude_restriction = f"5000ft|{no1_altitude1}ft"
                                    typical_altitude = f"5000ft"
                        case '+':
                            no1_altitude_restriction = f"{no1_altitude1}ft|{no1_altitude1 + 3000}ft"
                            typical_altitude = f"{no1_altitude1}ft"
                        case _:
                            match no2_altitude_description:
                                case 'B':
                                    no1_altitude_restriction = f"{no2_altitude2}ft|{no2_altitude1 + 2000}ft"
                                    typical_altitude = f"{no2_altitude2}ft"
                                case '-':
                                    no1_altitude_restriction = f"{no2_altitude1}ft|{int(no2_altitude1) + 2000}ft"
                                    typical_altitude = f"{no2_altitude1}ft"
                                case _:
                                    no1_altitude_restriction = f"{no2_altitude1}ft|{no2_altitude2}ft"
                                    typical_altitude = f"{no2_altitude1}ft"

                    no1_speed_limit = None_to_blank(star_legs[0][8])
                    no2_speed_limit = None_to_blank(star_legs[1][8])

                    speed_limit_temp = 300

                    if no1_speed_limit == '':
                        if no2_speed_limit == '':
                            no1_speed_restriction = f"90kt|{speed_limit_temp}kt"
                        else:
                            no1_speed_restriction = f"90kt|{no2_speed_limit}kt"

                    else:
                        no1_speed_restriction = f"90kt|{no1_speed_limit}kt"
                        speed_limit_temp = no1_speed_limit

                    approach_leg_element_list.extend([
                                                    f"false;{no1_waypoint_identifier}",
                                                    f"{no1_altitude_restriction}",
                                                    f"{no1_speed_restriction}",
                                                    f"{typical_altitude}"
                                                    ])
                    m = m + 1

                    if star_legs[num_of_wps_in_star_transitions - 1][4] == 'VTBS':
                        last_index = num_of_wps_in_star_transitions - 2
                    else:
                        last_index = num_of_wps_in_star_transitions - 1

                    while (m < last_index):
                        middle_waypoint_identifier = star_legs[m][4]
                        middle_altitude_description = None_to_blank(star_legs[m][5])

                        middle_altitude1 = None_to_blank(star_legs[m][6])
                        middle_altitude2 = None_to_blank(star_legs[m][7])

                        next_altitude_description = None_to_blank(star_legs[m + 1][5])
                        next_altitude1 = None_to_blank(star_legs[m + 1][6])
                        next_altitude2 = None_to_blank(star_legs[m + 1][7])

                        match middle_altitude_description:
                            case 'B':
                                middle_altitude_restriction = f"{middle_altitude2}ft|{middle_altitude1}ft"
                                typical_altitude = f"{middle_altitude2}ft"
                            case '-':
                                match no2_altitude_description:
                                    case '+':
                                        middle_altitude_restriction = f"{next_altitude1}ft|{middle_altitude1}ft"
                                        typical_altitude = f"{next_altitude1}ft"
                                    case _:
                                        middle_altitude_restriction = f"{middle_altitude1-2000}ft|{middle_altitude1}ft"
                                        typical_altitude = f"{middle_altitude1-2000}ft"
                            case '+':
                                middle_altitude_restriction = f"{middle_altitude1}ft|{middle_altitude1 + 2000}ft"
                                typical_altitude = f"{middle_altitude1}ft"
                            case '':
                                middle_altitude_restriction = f""
                                typical_altitude = f""

                        middle_speed_limit = None_to_blank(star_legs[m][8])

                        match middle_speed_limit:
                            case '':
                                middle_speed_restriction = ""
                            case _:
                                middle_speed_restriction = f"90kt|{middle_speed_limit}kt"
                                speed_limit_temp = middle_speed_limit

                        approach_leg_element_list.extend([
                                                f"{middle_waypoint_identifier}",
                                                f"{middle_altitude_restriction}",
                                                f"{middle_speed_restriction}",
                                                f"{typical_altitude}"
                                                ])
                        m = m + 1

                    last_waypoint_identifier = star_legs[last_index][4]
                    last_altitude_description = None_to_blank(star_legs[last_index][5])

                    last_altitude1 = None_to_blank(star_legs[last_index][6])
                    last_altitude2 = None_to_blank(star_legs[last_index][7])

                    postgres_sql_text = (f"SELECT DISTINCT airport_identifier,transition_identifier,waypoint_identifier,altitude_description,altitude1,altitude2,speed_limit "
                                         f" FROM airac_current.stars_wp"
                                         f" WHERE airport_identifier LIKE 'VT%' AND waypoint_icao_code LIKe 'VT'"
                                         f" AND transition_identifier LIKE '{transition_identifier}' "
                                         f" AND procedure_identifier LIKE '{procedure_identifier}' " 
                                         f" AND (waypoint_description_code LIKE '%EE%' AND route_type = '5') ;")

                    print(postgres_sql_text)
                    cursor_postgres.execute(postgres_sql_text)
                    final_leg = cursor_postgres.fetchall()

                    print(final_leg)
                    print(len(final_leg))

                    if len(final_leg) == 1:
                        FINAL_leg_altitude = None_to_blank(final_leg[0][4])
                        FINAL_leg_speed_limit = None_to_blank(final_leg[0][6])
                    else:
                        FINAL_leg_altitude = last_altitude1
                        FINAL_leg_speed_limit = 230

                    match last_altitude_description:
                        case 'B':
                            last_altitude_restriction = f"{last_altitude2}ft|{last_altitude1}ft"
                            typical_altitude = f"{last_altitude2}ft"
                        case '-':
                            last_altitude_restriction = f"{last_altitude1 - 1500}ft|{last_altitude1}ft"
                            typical_altitude = f"{last_altitude1 - 1500}ft"
                        case '+':
                            last_altitude_restriction = f"{last_altitude1}ft|{last_altitude1 + 2000}ft"
                            typical_altitude = f"{last_altitude1}ft"
                        case '':
                            match FINAL_leg_altitude:
                                case '':
                                    last_altitude_restriction = f"ft|ft"
                                    typical_altitude = f"ft"
                                case _:
                                    last_altitude_restriction = f"{FINAL_leg_altitude}ft|{FINAL_leg_altitude + 2000}ft"
                                    typical_altitude = f"{FINAL_leg_altitude}ft"

                    last_speed_limit = None_to_blank(star_legs[last_index][8])
                    print(FINAL_leg_altitude)

                    match last_speed_limit:
                        case '':
                            last_speed_restriction = f"90kt|{FINAL_leg_speed_limit}kt"
                        case _:
                            last_speed_restriction = f"90kt|{last_speed_limit}kt"

                    approach_leg_element_list.extend([
                                                f"{last_waypoint_identifier}",
                                                f"{last_altitude_restriction}",
                                                f"{last_speed_restriction}",
                                                f"{typical_altitude};false;;;;;;;LastLegLongestPath;Converted;false;TryVerticalFirst;;false"
                                                    ])

                    approach_leg.writerow(approach_leg_element_list)
