import psycopg2
import pandas as pd
from sqlalchemy import create_engine 

# Connection to database created in docker compose file
conn = psycopg2.connect(
    database='scrapped_data_database',
    user='postgres',
    password='postgres',
    host='0.0.0.0',
    port='8080',
)

# Cursor to perform operations
cursor = conn.cursor()

engine = create_engine(f'postgresql://postgres:postgres@0.0.0.0/scraped_data_database')

# Reading the result file
df = pd.read_csv('quandoo_berlin_results.csv')
# Populating the csv to the table
df.to_sql('berlin_restaurants_table', engine, if_exists='replace', index=False)

# Fetching the database
cursor.execute(
    """
SELECT *
FROM "berlin_restaurants_table"
WHERE "Restaurant_location" = 'Mitte'
LIMIT 50
"""
)
rows = cursor.fetchall()
print('The number of parts: ', cursor.rowcount)
for row in rows:
    print(row)

# Commit the changes and close the connection to the default database
conn.commit()
cursor.close()
conn.close()
