from sqlalchemy import create_engine
import psycopg2


database_url="postgresql+psycopg2://postgres:root@localhost/practice-ETL";

try:
 engine=create_engine(database_url)
 with engine.begin() as connection:
     print("Connected to the database")
 
except Exception as e:
    print("Error while connecting to the database:",str(e))
    
    
