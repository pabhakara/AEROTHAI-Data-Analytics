from subprocess import Popen, PIPE

cmd = "pg_dump --dbname=postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh --table=track.track_cat62_20240703 | psql -h localhost -W -U postgres temp"
print("Running:", cmd)

p = Popen(cmd, shell=True, stdin=PIPE)
p.communicate()
