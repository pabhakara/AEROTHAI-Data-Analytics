import os
import pandas as pd
import psycopg2
import datetime as dt
from subprocess import Popen, PIPE

# --- Configuration ---
remote_pg_uri = "postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh"
local_pg_user = "postgres"
local_pg_db = "temp"
local_pg_password = "password"
local_pg_host = "localhost"
local_pg_port = "5432"

# --- Date range ---
date_list = pd.date_range(start="2024-11-01", end="2024-11-30")

# --- Connect to local PostgreSQL ---
def table_exists_local(schema: str, table: str) -> bool:
    try:
        conn = psycopg2.connect(
            host=local_pg_host,
            port=local_pg_port,
            user=local_pg_user,
            password=local_pg_password,
            dbname=local_pg_db
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    );
                    """,
                    (schema, table)
                )
                return cur.fetchone()[0]
    except Exception as e:
        print(f"⚠️ Error checking local table {schema}.{table}:", e)
        return False

# --- Loop over dates ---
for date in date_list:
    yyyymmdd = date.strftime("%Y%m%d")

    schema = "sur_air" #"track" # "sur_air" #
    table = f"cat062_{yyyymmdd}" # f"track_cat62_{yyyymmdd}" #

    full_table = f"{schema}.{table}"

    print(f"\n📅 Checking table: {full_table}")

    if table_exists_local(schema, table):
        print(f"⏭️  Skipping {full_table} — already exists in local DB.")
        continue

    print(f"⬇️  Importing {full_table} from remote...")

    start_time = dt.datetime.now()
    print(f"⏱️  Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    command = (
        f"pg_dump --dbname={remote_pg_uri} "
        f"--table={full_table} | "
        f"psql -h {local_pg_host} -U {local_pg_user} -d {local_pg_db}"
    )

    env = os.environ.copy()
    env["PGPASSWORD"] = local_pg_password

    p = Popen(command, shell=True, env=env, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()

    end_time = dt.datetime.now()
    print(f"⏹️  End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    elapsed = (end_time - start_time).total_seconds()
    print(f"🕒 Duration: {elapsed:.2f} seconds")

    if p.returncode == 0:
        print(f"✅ Successfully imported {full_table}")
    else:
        print(f"❌ Failed to import {full_table}")
        print("STDERR:\n", stderr.decode())
