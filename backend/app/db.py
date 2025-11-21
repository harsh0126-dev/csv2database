# db.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL missing in .env")
print("\n\n📌 CONNECTING TO DATABASE:")
print(DATABASE_URL, "\n\n")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL, future=True)
