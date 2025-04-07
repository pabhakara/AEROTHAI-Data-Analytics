import os
import pandas as pd
from subprocess import Popen, PIPE

# --- Configuration ---
remote_pg_uri = "postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh"
local_pg_user = "postgres"
local_pg_db = "temp"
local_pg_password = "password"  # Your local PostgreSQL password

# --- Generate date list ---
date_list = pd.date_range(start="2025-01-20", end="2025-01-25")  # Change range as needed

# --- Loop over each date ---
for date in date_list:
    yyyymmdd = date.strftime("%Y%m%d")
    table_name = f"sur_air.cat062_{yyyymmdd}"

    print(f"\n📅 Transferring table: {table_name}")

    command = (
        f"pg_dump --dbname={remote_pg_uri} "
        f"--table={table_name} | "
        f"psql -h localhost -U {local_pg_user} -d {local_pg_db}"
    )

    # Set environment with password for non-interactive use
    env = os.environ.copy()
    env["PGPASSWORD"] = local_pg_password

    # Run the command
    p = Popen(command, shell=True, env=env, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()

    # Log result
    if p.returncode == 0:
        print(f"✅ Successfully transferred {table_name}")
    else:
        print(f"❌ Failed to transfer {table_name}")
        print("STDERR:\n", stderr.decode())
