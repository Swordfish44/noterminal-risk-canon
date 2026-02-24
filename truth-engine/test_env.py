import os
from dotenv import load_dotenv

load_dotenv()

print("ENV FOUND")
print("KEY:", os.getenv("OPENAI_API_KEY")[:10])
print("PG:", os.getenv("PG_CONN")[:35])


