from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

url = os.getenv("DATABASE_URL")
print("URL =", url)

engine = create_engine(url)

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("DB WORKING:", result.fetchone())
except Exception as e:
    print("ERROR:", e)
