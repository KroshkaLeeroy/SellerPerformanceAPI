import copy
import datetime
import os
from collections import deque
import json
from main_structure.new.utils import write_json, read_json
from application.config import ENCRYPTING_PASSWORD
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from base64 import urlsafe_b64encode, urlsafe_b64decode
import os
import json

TEMP = {
    'date_from': '',
    'date_to': '',
    'perf_id': '',
    'perf_api': '',
    'seller_id': '',
    'seller_secret': '',
    'user_id': '',
}

user = {
    "user_id": "",
    "last_report_id": 0,
    "total_request_count": 0,
    "history": []
}

history = {
    "user_id": "",
    "id": 0,
    "status": ("in_line", "ready"),
    "request_count": 0,
    "time_created": "yyyy-mm-dd",
    "time_from": "yyyy-mm-dd",
    "time_to": "yyyy-mm-dd",
}


def rebuild_history_total():
    print(f'Rebuilding history... {datetime.datetime.now()}')
    data = check_if_query_history_exists('history.json')
    for user_id in data['users']:
        for report in data['users'][user_id]['history']:
            if report['status'] == 'in_line':
                if os.path.exists(report["path"]):
                    report['status'] = 'ready'
                else:
                    report['status'] = 'some_error'
    write_json(data, 'history.json')


def rebuild_history_user(incom_user_id):
    data = check_if_query_history_exists('history.json')
    for report in data['users'][incom_user_id]['history']:
        if report['status'] == 'in_line':
            if os.path.exists(report["path"]):
                report['status'] = 'ready'
    write_json(data, 'history.json')


def check_if_query_history_exists(file_name):
    if os.path.exists(file_name):
        data = read_json(file_name)
        return data

    data = {'users': {}}
    write_json(data, file_name)
    return data


def add_user_query_to_history(file_name, query, path=None, status=None, date_created=False):
    data = check_if_query_history_exists(file_name)
    if query['user_id'] not in data['users']:
        data['users'][query['user_id']] = {
            "user_id": query['user_id'],
            "last_report_id": 0,
            "total_request_count": 0,
            "history": []
        }
    date = f'{query["date_from"]}_{query["date_to"]}'
    path = path if path else os.path.join('downloads', query['user_id'], date, date + '.xlsx')

    data['users'][query['user_id']]['history'].append({
        "user_id": query['user_id'],
        "id": len(data['users'][query['user_id']]['history']),
        "status": status if status else 'in_line',
        "request_count": 0,
        "time_created": query['date_create'] if date_created else datetime.datetime.now().strftime("%Y-%m-%d-%H-%M"),
        "time_from": query['date_from'],
        "time_to": query['date_to'],
        "path": path,
    })
    data['users'][query['user_id']]['last_report_id'] = len(data['users'][query['user_id']]['history']) - 1
    write_json(data, file_name)


def decrypt_data(key: bytes, encrypted_json: str):
    encrypted_data = json.loads(encrypted_json)
    iv = urlsafe_b64decode(encrypted_data['iv'])
    cipher_data = urlsafe_b64decode(encrypted_data['data'])

    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    decryptor = cipher.decryptor()
    padded_data = decryptor.update(cipher_data) + decryptor.finalize()

    # Удаление отступов
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    json_data = unpadder.update(padded_data) + unpadder.finalize()

    # Возвращение данных в зависимости от их типа
    if encrypted_data['type'] == 'json':
        return json.loads(json_data.decode('utf-8'))
    elif encrypted_data['type'] == 'string':
        return json_data.decode('utf-8')
    else:
        raise ValueError("Unknown data type")


def encrypt_data(key: bytes, data) -> str:
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # Проверка типа данных и сериализация
    if isinstance(data, dict):
        json_data = json.dumps(data).encode()
    elif isinstance(data, str):
        json_data = data.encode()
    else:
        raise ValueError("Data must be a dictionary or a string")

    # Добавление отступов для соответствия блочному шифру
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(json_data) + padder.finalize()

    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    encrypted_json = {
        'iv': urlsafe_b64encode(iv).decode('utf-8'),
        'data': urlsafe_b64encode(encrypted_data).decode('utf-8'),
        'type': 'json' if isinstance(data, dict) else 'string'
    }
    return json.dumps(encrypted_json)


def update_user_query_in_history(file_name, user_id, status, request_count=0):
    data = check_if_query_history_exists(file_name)
    data['users'][user_id]['history'][-1]['status'] = status
    data['users'][user_id]['history'][-1]['request_count'] = request_count
    write_json(data, file_name)


class Queue:
    def __init__(self):
        self.queue = deque()

    def enqueue(self, item):
        self.queue.append(item)

    def dequeue(self):
        if not self.is_empty():
            return self.queue.popleft()

    def is_empty(self):
        return len(self.queue) == 0


def save_queue(queue, filename):
    with open(filename, 'w') as f:
        json.dump(list(queue.queue), f)


def load_queue(filename):
    try:
        with open(filename, 'r') as f:
            items = json.load(f)
            queue = Queue()
            for item in items:
                queue.enqueue(item)
            return queue
    except FileNotFoundError:
        return Queue()
