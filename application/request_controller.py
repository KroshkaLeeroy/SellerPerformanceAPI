import os
import time
import traceback
import datetime
from main_structure.new.merger.merger import Merger
from main_structure.new.utils import read_json
from application.utils import load_queue, save_queue, update_user_query_in_history


class ControllerRequests:
    def __init__(self):
        self.file_name = 'queue.json'
        self.queue = load_queue(self.file_name)

    def main_cycle(self):
        while True:
            if not self.queue.is_empty():
                current_request = self.queue.dequeue()
                merger = Merger(
                    current_request['date_from'],
                    current_request['date_to'],
                    current_request['perf_id'],
                    current_request['perf_api'],
                    current_request['seller_id'],
                    current_request['seller_secret'],
                    current_request['user_id']
                )
                try:
                    merger.run()
                    date = current_request['date_from'] + '_' + current_request['date_to']
                    path = os.path.join('downloads', current_request['user_id'], date, 'stat', 'download',
                                        'main') + '.json'
                    try:
                        requests_count = read_json(path)['total_requests']
                    except FileNotFoundError:
                        requests_count = 0
                    update_user_query_in_history('history.json', current_request['user_id'], 'ready', requests_count)
                    save_queue(self.queue, self.file_name)
                except Exception:
                    text = (f"Date: {datetime.datetime.now()}\n" + f"User: {current_request['user_id']}\n"
                            + f"{traceback.format_exc()}\n\n")

                    with open('critical_error.txt', 'a', encoding='utf-8') as file:
                        file.write(text)

                    path = os.path.join(merger.client_folder_path, merger.date, 'error_text.txt')

                    with open(path, 'a', encoding='utf-8') as file:
                        file.write(text)

                    update_user_query_in_history('history.json', current_request['user_id'], 'some_error')
                    save_queue(self.queue, self.file_name)
            else:
                time.sleep(30)
