import psycopg2
import psycopg2.extras

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

schema = "jade_mk"

# --- Connect ---
remote_conn = psycopg2.connect(**remote_config)
local_conn = psycopg2.connect(**local_config)

remote_cur = remote_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
local_cur = local_conn.cursor()

def find_timestamp_column(cursor, schema: str, table: str) -> str:
    """Return the name of the most suitable timestamp column."""
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND data_type IN ('timestamp without time zone', 'timestamp with time zone')
        ORDER BY 
            CASE 
                WHEN column_name ILIKE 'last_updated' THEN 1
                WHEN column_name ILIKE 'updated_at' THEN 2
                WHEN column_name ILIKE 'created_at' THEN 3
                ELSE 100
            END;
    """
    cursor.execute(query, (schema, table))
    result = cursor.fetchone()
    return result[0] if result else None

# --- Get all table names in the schema ---
remote_cur.execute(f"""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = %s AND table_type = 'BASE TABLE'
    ORDER BY table_name;
""", (schema,))
tables = [row[0] for row in remote_cur.fetchall()]
print(f"📋 Found {len(tables)} tables in schema '{schema}'")

# --- Loop through tables ---
for table in tables:
    print(f"\n🔄 Processing table: {schema}.{table}")

    try:
        ts_column = find_timestamp_column(remote_cur, schema, table)
        if not ts_column:
            print(f"⚠️ Skipping — no timestamp column in {schema}.{table}")
            continue

        print(f"🕒 Using timestamp column: {ts_column}")

        # Step 1: Get latest timestamp in local
        try:
            local_cur.execute(f"""
                SELECT MAX({ts_column}) FROM {schema}.{table}
            """)
            latest_ts = local_cur.fetchone()[0]
        except Exception as e:
            print(f"⚠️ Cannot read local max timestamp: {e}")
            local_conn.rollback()
            continue

        print(f"📌 Latest local timestamp: {latest_ts if latest_ts else 'None (inserting all)'}")

        # Step 2: Fetch new records from remote
        if latest_ts:
            remote_cur.execute(f"""
                SELECT * FROM {schema}.{table}
                WHERE {ts_column} > %s
            """, (latest_ts,))
        else:
            remote_cur.execute(f"SELECT * FROM {schema}.{table}")

        rows = remote_cur.fetchall()
        columns = [desc.name for desc in remote_cur.description]
        print(f"➕ {len(rows)} new rows to insert")

        # Step 3: Insert into local
        if rows:
            insert_sql = f"""
                INSERT INTO {schema}.{table} ({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(columns))})
            """
            psycopg2.extras.execute_batch(local_cur, insert_sql, rows)
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
print("\n✅ Timestamp-based sync complete.")
