import datetime
import os
from collections import deque
import json
from main_structure.new.utils import write_json, read_json

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


def rebuild_history():
    data = check_if_query_history_exists('history.json')
    for user_id in data['users']:
        for report in data['users'][user_id]['history']:
            if report['status'] == 'in_line':
                report['status'] = 'some_error'
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
