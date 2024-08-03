import os
from dotenv import load_dotenv


load_dotenv()

DB_RUS = os.getenv("DB_RUS")
DB_TO = os.getenv("DB_TO")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")


# engines for work with project's databases:
DATABASE_FROM = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_RUS}"
DATABASE_TO = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_TO}"
