from subprocess import PIPE,Popen

# def dump_table(host_name,database_name,user_name,database_password,table_name):
#
#     command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t public.{3} -Fc -f /tmp/table.dmp'\
#     .format(host_name,database_name,user_name,table_name)
#
#     p = Popen(command,shell=True,stdin=PIPE)
#
#     return p.communicate('{}\n'.format(database_password))
#
# def restore_table(host_name,database_name,user_name,database_password):
#
#     command = 'pg_restore -h {0} -d {1} -U {2} < /tmp/table.dmp'\
#     .format(host_name,database_name,user_name)
#
#     p = Popen(command,shell=True,stdin=PIPE)
#
#     return p.communicate('{}\n'.format(database_password))
#
# def main():
#     dump_table('localhost','testdb','user_name','passwd','test_tbl')
#  #   restore_table('remotehost','new_db','user_name','passwd')

command = f"pg_dump --dbname=postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh --table=track.track_cat62_20240119 | psql -h " \
f"localhost -W -U postgres temp"

print(command)

p = Popen(command,shell=True,stdin=PIPE)
p.communicate()

# if __name__ == "__main__":
#     main()