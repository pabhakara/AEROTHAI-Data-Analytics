import os
import datetime as dt
from subprocess import Popen, PIPE

# --- Configuration ---
remote_pg_uri = "postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh"
local_pg_user = "postgres"
local_pg_db = "temp"
local_pg_password = "password"
local_pg_host = "localhost"
local_pg_port = "5432"
schema = "jade_mk"  # Change this to the schema you want to import

print(f"\n📦 Starting full schema import (overwrite): {schema}")

start_time = dt.datetime.now()
print(f"⏱️  Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

# --- pg_dump with clean option ---
command = (
    f"pg_dump --clean --if-exists --dbname={remote_pg_uri} "
    f"--schema={schema} | "
    f"psql -h {local_pg_host} -U {local_pg_user} -d {local_pg_db}"
)

env = os.environ.copy()
env["PGPASSWORD"] = local_pg_password

# --- Run the command ---
p = Popen(command, shell=True, env=env, stdin=PIPE, stdout=PIPE, stderr=PIPE)
stdout, stderr = p.communicate()

end_time = dt.datetime.now()
print(f"⏹️  End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
elapsed = (end_time - start_time).total_seconds()
print(f"🕒 Duration: {elapsed:.2f} seconds")

# --- Check result ---
if p.returncode == 0:
    print(f"✅ Successfully imported (overwritten) schema: {schema}")
    print("STDOUT:\n", stdout.decode())
else:
    print(f"❌ Failed to import schema: {schema}")
    print("STDERR:\n", stderr.decode())
    print("STDOUT:\n", stdout.decode())
