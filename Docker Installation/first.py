import os
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
print(f"Database Host: {db_host}, Port: {db_port}")

if db_host:
    print('hello from waleed'
    )