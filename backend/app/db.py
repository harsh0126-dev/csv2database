# db.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Path of backend/app folder
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Move TWO levels up: backend/app/../../ = project root
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

# .env should be here
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")

print("📌 CURRENT_DIR =", CURRENT_DIR)
print("📌 PROJECT_ROOT =", PROJECT_ROOT)
print("📌 Looking for .env at =", ENV_PATH)

# Attempt to load .env
loaded = load_dotenv(ENV_PATH)
print("📌 load_dotenv returned =", loaded)

DATABASE_URL = os.getenv("DATABASE_URL")
print("📌 DATABASE_URL loaded as =", DATABASE_URL)

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL missing! Could not load from .env")

engine = create_engine(DATABASE_URL, future=True)
