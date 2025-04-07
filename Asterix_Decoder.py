import asterix
import csv

path = "/Users/pongabha/Library/CloudStorage/Dropbox/Workspace/BearCat Aviation/Bhutan/ADS-B Data Sample/JAN/"

filename = "TP1_20250114_050000_Cat21_Cat23_Cat25_Cat247.ast"

input_file = f"{path}{filename}"

with open(input_file, "rb") as f:
    binary_data = f.read()

# Decode the ASTERIX data
decoded_data = asterix.parse(binary_data)

print(decoded_data)
print(asterix.describe(decoded_data))