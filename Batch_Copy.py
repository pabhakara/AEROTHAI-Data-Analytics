import subprocess

REMOTE_CONN = "postgres://de_old_data:de_old_data@172.16.129.241:5432/aerothai_dwh"
LOCAL_DB = "temp"
LOCAL_USER = "postgres"

# Step 1: Query for matching tables
sql = """
SELECT 'sur_air.' || tablename
FROM pg_tables
WHERE schemaname = 'sur_air' AND tablename LIKE 'cat062_2025012%';
"""

res = subprocess.run(
    ["psql", REMOTE_CONN, "-At", "-c", sql],
    capture_output=True,
    text=True
)

tables = [line.strip() for line in res.stdout.strip().split('\n') if line.strip()]
if not tables:
    print("No matching tables found.")
    exit()

# Step 2: Build the dump command
pg_dump_cmd = ["pg_dump", REMOTE_CONN] + [f"--table={t}" for t in tables]

# Final full command
command = " ".join(pg_dump_cmd) + f" | PGPASSWORD=your_local_password psql -h localhost -U {LOCAL_USER} {LOCAL_DB}"

# Step 3: Run it
print(f"Running: {command}")
subprocess.run(command, shell=True)
