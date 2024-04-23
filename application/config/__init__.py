import os
from dotenv import load_dotenv, find_dotenv

if not find_dotenv():
    exit("Переменные окружение не загружены, так как отсутствует файл .env")
else:
    load_dotenv()

SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
SESSION_TYPE = os.getenv('SESSION_TYPE')
DEBUG = os.getenv('DEBUG')
URL_TO_API = os.getenv('URL_TO_API')
ADMIN_KEY = os.getenv('ADMIN_KEY')
