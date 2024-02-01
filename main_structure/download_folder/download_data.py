import datetime

from .utils import get_token, get_list_of_ids, split_list, get_uuid, check_uuid_status, download_file, \
    ensure_directory_exists, delete_directory
from .utils import logging, os
import time


class DownloadZip:
    def __init__(self, date_from, date_to, type_, client_id, client_secret, client_folder="NO_ID_FOUND",
                 debug=False):
        """
        :param date_from: 2023-09-20 ГГГГ:ММ:ДД
        :param date_to: 2023-09-20 ГГГГ:ММ:ДД
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_folder = client_folder
        self.files_path = os.path.join('downloads', client_folder, f'{date_from}_{date_to}', type_)
        self.date_from = date_from
        self.date_to = date_to
        self.type_ = type_
        self.debug = debug

        self.UUID_error_request_set = []

        self.any_errors = False
        self.query_counter = 0

    def debug_(self, text):
        if self.debug:
            print(text)

    def download_files(self, force_push=False):

        directory_status = ensure_directory_exists(self.files_path)
        if not directory_status:
            logging.warning(
                f"Директория для загрузки файлов уже существует или не удалось ее создать. {self.files_path}")
            self.debug_(f"Директория для загрузки файлов уже существует или не удалось ее создать. {self.files_path}")

            if force_push:
                status_delete = delete_directory(self.files_path)
                if status_delete:
                    logging.info(
                        f"Принудительная очистка и создание директории с загрузкой. {self.files_path}")
                    self.debug_(f"Принудительная очистка и создание директории с загрузкой. {self.files_path}")
                    ensure_directory_exists(self.files_path)
            else:
                return

        token = get_token(self.client_id, self.client_secret)
        self.query_counter += 1

        if not token:
            logging.warning(f"Не удалось получить токен по клиенту {self.client_folder}")
            self.debug_("Не удалось получить токен")
            self.any_errors = True
            return

        self.debug_(f"Токен успешно получен: {token}")
        list_of_id = get_list_of_ids(token, self.type_)
        self.query_counter += 1

        if not list_of_id:
            logging.warning(f"Не удалось получить список идентификаторов по клиенту {self.client_folder}")
            self.debug_("Не удалось получить список идентификаторов")
            self.any_errors = True
            return

        logging.info(f"По клиенту {self.client_folder} список идентификаторов успешно получен: {list_of_id}")
        self.debug_(f"Список идентификаторов успешно получен: {list_of_id}")
        split_list_of_id = split_list(list_of_id, 10)

        counter = 0

        for list_ in split_list_of_id:
            file_type = "zip" if len(list_) > 1 else "csv"

            uuid_status, uuid = self.get_uuid(token, list_)
            if not uuid_status:
                continue

            file_path = os.path.join(self.files_path, f"{counter}-{self.type_}.{file_type}")

            time.sleep(5)

            status = self.get_uuid_status(uuid, token)

            if not status:
                continue

            result = download_file(token, uuid, file_path)
            self.query_counter += 1
            if not result[0]:
                self.debug_(f"Не удалось скачать отчеты ошибка {result[1]} по компаниям: {list_}")
                self.any_errors = True
            else:
                self.debug_('Скачаны отчеты по компаниям: ' + str(list_))
            counter += 1
        logging.info(
            f"По клиенту {self.client_folder} отчеты скачаны, пропущены следующие: {self.UUID_error_request_set}")

    def get_uuid_status(self, uuid, token):
        status = check_uuid_status(uuid, token)
        attempt = 30
        while status == "IN_PROGRESS" and attempt > 0:
            logging.info(
                f'По клиенту {self.client_folder}: Попытка запросить статус UUID. Осталось попыток: ' + str(attempt))
            self.debug_('Попытка запросить статус UUID. Осталось попыток: ' + str(attempt))
            time.sleep(10)
            status = check_uuid_status(uuid, token)
            self.query_counter += 1
            attempt -= 1

        if status == "OK":
            logging.info(f"По клиенту {self.client_folder}: Статус UUID готов к скачиванию")
            self.debug_("Статус UUID готов к скачиванию")
            return True
        elif status == "ERROR":
            logging.error(
                f'По клиенту {self.client_folder}: Запрос статуса UUID выдал ошибку смотрим логи, время {datetime.datetime.now()}')
            self.debug_(f"Запрос статуса UUID выдал ошибку, смотрим логи, время {datetime.datetime.now()}")
            return False

    def get_uuid(self, token, list_of_id):
        time.sleep(5)
        for attempt in range(5):
            logging.info(
                f"По клиенту {self.client_folder}: попытка {attempt + 1} запросить UUID компаний {list_of_id}")
            self.debug_(f"Попытка {attempt + 1} запросить UUID компаний {list_of_id}")
            uuid = get_uuid(token, self.date_from, self.date_to, list_of_id)
            self.query_counter += 1
            if uuid:
                logging.info(f"По клиенту {self.client_folder} UUID получен: {uuid}")
                self.debug_(f"По клиенту {self.client_folder} UUID получен: {uuid}")
                return True, uuid
            time.sleep(10)

        logging.error(f"По клиенту {self.client_folder} не удалось запросить UUID {list_of_id}")
        self.debug_(f"Не удалось запросить UUID {list_of_id}")
        self.UUID_error_request_set.append(list_of_id)
        return False, None
