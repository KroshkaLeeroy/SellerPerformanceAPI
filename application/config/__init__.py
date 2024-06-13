import os
from dotenv import load_dotenv, find_dotenv

from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend


def generate_key(password: str, salt) -> bytes:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,  # 16-bytes of salt
        iterations=100000,
        backend=default_backend()
    )
    return kdf.derive(password.encode())


if not find_dotenv():
    exit("Переменные окружение не загружены, так как отсутствует файл .env")
else:
    load_dotenv()

SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
SESSION_TYPE = os.getenv('SESSION_TYPE')
DEBUG = os.getenv('DEBUG')
URL_TO_API = os.getenv('URL_TO_API')
ADMIN_KEY = os.getenv('ADMIN_KEY')
ENCRYPTING_KEY = os.getenv('ENCRYPTING_KEY')
SALT_KEY = os.getenv('SALT_KEY').encode()

ENCRYPTING_PASSWORD = generate_key(ENCRYPTING_KEY, SALT_KEY)
