import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

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

engine = create_engine(f'postgresql://postgres:postgres@0.0.0.0/scrapped_data_database')

# Reading the result file
df = pd.read_csv('quandoo_berlin_results.csv')
df.to_sql('berlin_restaurants_table', engine, if_exists='replace', index=False)


# Commit the changes and close the connection to the default database
conn.commit()
cursor.close()
conn.close()
