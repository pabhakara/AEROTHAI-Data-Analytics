import psycopg2

# --- Config ---
db_config = {
    "host": "localhost",
    "port": "5432",
    "database": "navigraph",  # Change if you're running on Navigraph, etc.
    "user": "postgres",
    "password": "password"
}

schema_current = "airac_current"
schema_previous = "airac_previous"
schema_added = "airac_added"
schema_subtracted = "airac_subtracted"

# --- Connect ---
conn = psycopg2.connect(**db_config)
conn.autocommit = True
cursor = conn.cursor()

# --- Create output schemas if they don’t exist ---
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_added};")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_subtracted};")

# --- Get tables from airac_current ---
cursor.execute(f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = %s AND table_type = 'BASE TABLE';
""", (schema_current,))
tables = [row[0] for row in cursor.fetchall()]

# --- Compare and populate airac_added / airac_subtracted ---
for table in tables:
    print(f"\n🔍 Processing table: {table}")

    # Check if it exists in airac_previous
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
    """, (schema_previous, table))
    exists = cursor.fetchone()[0]

    if not exists:
        print(f"⚠️  Table {table} not found in {schema_previous}, skipping.")
        continue

    # --- airac_added.{table} ---
    print(f"➕ Creating {schema_added}.{table}")
    cursor.execute(f'DROP TABLE IF EXISTS {schema_added}."{table}";')
    cursor.execute(f"""
        CREATE TABLE {schema_added}."{table}" AS
        SELECT * FROM {schema_current}."{table}"
        EXCEPT
        SELECT * FROM {schema_previous}."{table}";
    """)

    # --- airac_subtracted.{table} ---
    print(f"➖ Creating {schema_subtracted}.{table}")
    cursor.execute(f'DROP TABLE IF EXISTS {schema_subtracted}."{table}";')
    cursor.execute(f"""
        CREATE TABLE {schema_subtracted}."{table}" AS
        SELECT * FROM {schema_previous}."{table}"
        EXCEPT
        SELECT * FROM {schema_current}."{table}";
    """)

    print(f"✅ Done: {schema_added}.{table} & {schema_subtracted}.{table}")

# --- Clean up ---
cursor.close()
conn.close()

print("\n✅ All tables processed.")
