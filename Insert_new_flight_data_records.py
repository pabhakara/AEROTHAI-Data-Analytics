import psycopg2
import psycopg2.extras
import subprocess
import shutil
import json

def sanitize_row(row):
    return [json.dumps(val) if isinstance(val, dict) else val for val in row]


def ensure_local_table_exists(schema: str, table: str):
    """Check if local table exists; if not, copy DDL from remote using pg_dump."""
    try:
        # Try to SELECT from the table
        local_cur.execute(f'SELECT 1 FROM "{schema}"."{table}" LIMIT 1')
        return  # ✅ Table exists
    except psycopg2.errors.UndefinedTable:
        local_conn.rollback()
        print(f"⚙️  Local table {schema}.{table} does not exist. Creating via pg_dump...")

    # Locate pg_dump
    pg_dump_path = shutil.which("pg_dump")
    if not pg_dump_path:
        raise FileNotFoundError("❌ pg_dump not found in system PATH.")

    try:
        # Prepare pg_dump command
        cmd = [
            pg_dump_path,
            f"--host={remote_config['host']}",
            f"--port={remote_config['port']}",
            f"--username={remote_config['user']}",
            "--no-password",
            "--schema-only",
            f"--table={schema}.{table}",
            remote_config["dbname"]
        ]

        # Run pg_dump and capture output
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"PGPASSWORD": remote_config["password"]}
        )
        ddl, err = proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(f"pg_dump error:\n{err.decode()}")

        ddl_str = ddl.decode()
        local_cur.execute(f"CREATE SCHEMA IF NOT EXISTS \"{schema}\"")
        local_conn.commit()
        local_cur.execute(ddl_str)
        local_conn.commit()
        print(f"✅ Local table {schema}.{table} created.")

    except Exception as e:
        print(f"❌ Failed to create table {schema}.{table}: {e}")
        local_conn.rollback()


# --- Config ---
remote_config = {
    "host": "172.16.129.241",
    "port": "5432",
    "user": "de_old_data",
    "password": "de_old_data",
    "dbname": "aerothai_dwh"
}

local_config = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "password",
    "dbname": "temp"
}

schema = "flight_data"

# --- Connect ---
remote_conn = psycopg2.connect(**remote_config)
local_conn = psycopg2.connect(**local_config)

remote_cur = remote_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
local_cur = local_conn.cursor()

# --- Get all table names in the schema ---
remote_cur.execute(f"""
    SELECT table_name FROM information_schema.tables 
    WHERE table_name = 'flight_202504' AND table_schema = %s AND table_type = 'BASE TABLE'
    ORDER BY table_name;
""", (schema,))
tables = [row[0] for row in remote_cur.fetchall()]
print(f"📋 Found {len(tables)} tables in schema '{schema}'")

# --- Loop through tables ---
for table in tables:
    print(f"\n🔄 Processing table: {schema}.{table}")

    ensure_local_table_exists(schema, table)
    id_column = "id"


    try:
        # Step 1: Get latest ID in local
        try:
            local_cur.execute(f'SELECT MAX("{id_column}") FROM "{schema}"."{table}"')
            latest_id = local_cur.fetchone()[0]
        except Exception as e:
            print(f"⚠️ Cannot read local max ID: {e}")
            local_conn.rollback()
            latest_id = None

        print(f"📌 Latest local ID: {latest_id if latest_id else 'None (inserting all)'}")

        # Step 2: Fetch new records from remote
        if latest_id:
            remote_cur.execute(f'SELECT * FROM "{schema}"."{table}" WHERE "{id_column}" > %s', (latest_id,))
        else:
            remote_cur.execute(f'SELECT * FROM "{schema}"."{table}"')

        rows = remote_cur.fetchall()
        columns = [desc.name for desc in remote_cur.description]
        print(f"➕ {len(rows)} new rows to insert")

        # Step 3: Insert into local
        if rows:
            columns = [desc.name for desc in remote_cur.description]

            # Remove 'id' from insert if it is auto-generated in the local DB
            if 'id' in columns:
                id_index = columns.index('id')
                columns.remove('id')
                rows = [tuple([v for i, v in enumerate(row) if i != id_index]) for row in rows]

            col_names = ', '.join([f'"{col}"' for col in columns])
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = f'INSERT INTO "{schema}"."{table}" ({col_names}) VALUES ({placeholders})'

            sanitized_rows = [sanitize_row(row) for row in rows]
            psycopg2.extras.execute_batch(local_cur, insert_sql, sanitized_rows)

            local_conn.commit()
            print(f"✅ Inserted {len(rows)} rows into {schema}.{table}")
        else:
            print("ℹ️ No new data.")

    except Exception as e:
        print(f"❌ Error processing {schema}.{table}: {e}")
        local_conn.rollback()

# --- Close ---
remote_cur.close()
local_cur.close()
remote_conn.close()
local_conn.close()
print("\n✅ ID-based sync complete.")
