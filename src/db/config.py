import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
host = os.getenv("HOST")
port = os.getenv("PORT")
database_name = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"

# DATABASE_URL = 'postgresql://username:password@localhost:5432/database_name'

engine = create_engine(DATABASE_URL)

