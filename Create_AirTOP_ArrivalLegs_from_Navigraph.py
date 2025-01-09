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
                             f" WHERE airport_identifier LIKE 'VT%' AND waypoint_icao_code LIKe 'VT'"
                             f" AND NOT ((waypoint_description_code LIKE '%EE%' OR seqno = 10) AND route_type = '5') "
                             f" GROUP BY airport_identifier, procedure_identifier,route_type,transition_identifier"
                             f" ORDER BY airport_identifier, procedure_identifier,route_type;")
        print(postgres_sql_text)

        cursor_postgres.execute(postgres_sql_text)
        star_transitions = cursor_postgres.fetchall()

        num_of_star_transitions = len(star_transitions)

        print(star_transitions)
        print(num_of_star_transitions)

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

            if num_of_wps_in_star_transitions == 2:
                
                m = 0

                postgres_sql_text = (f"SELECT airport_identifier,procedure_identifier,transition_identifier,seqno,"
                                     f" waypoint_identifier,altitude_description,altitude1,altitude2,speed_limit "
                                     f" FROM airac_current.stars_wp"
                                     f" WHERE airport_identifier LIKE '{airport_identifier}' "
                                     f" AND procedure_identifier LIKE '{procedure_identifier}' "
                                     f" AND transition_identifier LIKE '{transition_identifier}' "
                                     f" AND waypoint_icao_code LIKe 'VT'"
                                     f" AND NOT ((waypoint_description_code LIKE '%EE%' OR seqno = 10) AND route_type = '5')  "
                                     f" ORDER BY airport_identifier, procedure_identifier,route_type,transition_identifier,seqno;")

                print(postgres_sql_text)

                cursor_postgres.execute(postgres_sql_text)
                star_legs = cursor_postgres.fetchall()
                
                approach_leg_header = f"{airport_identifier}.{procedure_identifier}.{transition_identifier};true;;;;;;;;;"
                next_approach_leg =  f"{airport_identifier}.{procedure_identifier}.FINAL"
                
                approach_leg_element_list = []
                
                approach_leg_element_list.append(f"{approach_leg_header}{next_approach_leg}")
                
                approach_leg_element_list.extend(['None','','','','','','None','','','','','','','FALSE','BeginToEnd',''])
                
                no1_waypoint_identifier = star_legs[0][4]
                
                no1_altitude_description = None_to_blank(star_legs[0][5])
                no1_altitude1 = None_to_blank(star_legs[0][6])
                no1_altitude2 = None_to_blank(star_legs[0][7])
                
                if no1_altitude_description == 'B':
                    lower_altitude = no1_altitude1
                    upper_altitude = no1_altitude2
                    
                if middle_altitude_description == '-':
                    lower_altitude = ''
                    upper_altitude = no1_altitude1
                    
                if middle_altitude_description == '+':
                    lower_altitude = no1_altitude1
                    upper_altitude = ''
                
                no1_speed_limit = None_to_blank(star_legs[0][8])
                
                approach_leg_element_list.extend([
                                                f"false;{no1_waypoint_identifier}",
                                                f"{no1_altitude1}ft|FL200",
                                                f"90kt|{no1_speed_limit}kt",
                                                f"{no1_altitude1}ft"
                                                ])
                m = m + 1
                
                while m < num_of_wps_in_star_transitions - 1:
                    middle_waypoint_identifier = star_legs[m][4]
                    middle_altitude_description = None_to_blank(star_legs[m][5])
                    
                    middle_altitude1 = None_to_blank(star_legs[m][6])
                    middle_altitude2 = None_to_blank(star_legs[m][7])
                    
                    if middle_altitude_description == 'B':
                        lower_altitude = middle_altitude1
                        upper_altitude = middle_altitude2
                    
                    if middle_altitude_description == '-':
                        lower_altitude = ''
                        upper_altitude = middle_altitude1
                    
                    if middle_altitude_description == '+':
                        lower_altitude = middle_altitude1
                        upper_altitude = ''
                
                    
                    middle_speed_limit = None_to_blank(star_legs[m][8])
                    
                    approach_leg_element_list.extend([
                                            f"false;{middle_waypoint_identifier}",
                                            f"{lower_altitude}ft|{upper_altitude}ft",
                                            f"90kt|{middle_speed_limit}kt",
                                            f"{lower_altitude}ft"
                                            ])
                    m = m + 1
                    
                last_index = num_of_wps_in_star_transitions - 1
                
                last_waypoint_identifier = star_legs[last_index][4]
                last_altitude_description = None_to_blank(star_legs[last_index][5])
                
                last_altitude1 = None_to_blank(star_legs[last_index][6])
                last_altitude2 = None_to_blank(star_legs[last_index][7])
                
                if last_altitude_description == 'B':
                    lower_altitude = last_altitude1
                    upper_altitude = last_altitude2
                if middle_altitude_description == '-':
                    lower_altitude = ''
                    upper_altitude = last_altitude1
                if middle_altitude_description == '+':
                    lower_altitude = last_altitude1
                    upper_altitude = ''
                
                last_speed_limit = None_to_blank(star_legs[last_index][8])

                approach_leg_element_list.extend([
                                                f"{last_waypoint_identifier}",
                                                f"{lower_altitude}ft|{upper_altitude}",
                                                f"90kt|{last_speed_limit}kt",
                                                f"{lower_altitude}fft;false;;;;;;;LastLegLongestPath;Converted;false;TryVerticalFirst;;false"
                                                ])
                                                
                approach_leg.writerow(approach_leg_element_list)

                approach_leg.writerow([f"{approach_leg_header}"
                                       f"{next_approach_leg}",
                                        'None','','','','','','None','','','','','','','FALSE','BeginToEnd','',
                                        f"false;{no1_waypoint_identifier}",
                                        f"{no1_altitude1}ft|FL200",
                                        f"90kt|{no1_speed_limit}kt",
                                        f"{no1_altitude1}ft",
                                           
                                           f"{no2_waypoint_identifier}",
                                           f"{no2_altitude1}ft|FL200",
                                           f"90kt|{no2_speed_limit}kt",
                                           f"{no2_altitude1}fft;false;;;;;;;LastLegLongestPath;Converted;false;TryVerticalFirst;;false"
                                          ])