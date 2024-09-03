from dotenv import load_dotenv
import os

load_dotenv()

CITIES_FILE = os.getenv("CITIES_FILE")
API_KEY = os.getenv("API_KEY")
STORAGE_BUCKET_NAME = os.getenv("STORAGE_BUCKET_NAME")
