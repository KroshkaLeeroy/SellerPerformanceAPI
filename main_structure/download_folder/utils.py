from __future__ import annotations

import shutil

import requests
import json
import logging
import os

from main_structure.download_folder.config import get_token_params, get_list_of_id_params, get_uuid_params, \
    get_check_uuid_status_params, \
    get_download_file_params

# Настройка логирования

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    encoding='utf-8')


def get_token(client_id: str, client_secret: str) -> str | None:
    """
    Получает токен доступа, выполняя POST-запрос по-указанному URL с заданными заголовками и параметрами.

    Возвращает:
        str: Токен доступа, если запрос успешен.
        None: Если произошла ошибка при выполнении запроса или возникло исключение.
    """
    try:
        get_dict = get_token_params(client_id, client_secret)

        url = get_dict.get("url")
        headers = get_dict.get("headers")
        params = get_dict.get("params")

        data = json.dumps(params)

        result = requests.post(url=url, headers=headers, data=data)
        result.raise_for_status()  # Проверяем успешность запроса, поднимаем исключение при ошибке HTTP

        token = result.json().get('access_token')
        logging.info("Токен успешно получен.")
        return token

    except requests.RequestException as req_err:
        logging.error(f"Ошибка запроса при получении токена: {result.text} * {result.status_code} * {req_err}")
        return None

    except Exception as e:
        logging.error(f"Необработанная ошибка при получении токена: {e}")
        return None


def get_list_of_ids(token: str, type_: str) -> None | list:
    """
    Функция для получения списка идентификаторов на основе заданного токена и типа.

    Аргументы:
        token (str): Токен аутентификации.
        type_ (str): Тип идентификаторов для получения. Может быть "SKU" или "PROMO".

    Возвращает:
        list or None: Список идентификаторов, если запрос успешен и тип валиден, в противном случае None.

    Вызывает:
        requests.RequestException: Если произошла ошибка с HTTP-запросом.
        Exception: Если произошла непредвиденная ошибка.
    """
    try:
        params = get_list_of_id_params(token)

        response = requests.get(url=params["url"], headers=params["headers"], json=params["params"])
        response.raise_for_status()

        json_dict = response.json()["list"]
        sku = [campaign["id"] for campaign in json_dict if campaign["advObjectType"] == "SKU"]
        search_promo = [campaign["id"] for campaign in json_dict if campaign["advObjectType"] == "SEARCH_PROMO"]

        if type_ == "SKU":
            return sku

        if type_ == "PROMO":
            return search_promo

    except requests.RequestException as req_err:
        logging.error(f"Ошибка запроса при получении списка ID по компаниям: {response.text} * {response.status_code} * {req_err}")
        return None
    except Exception as e:
        logging.error(f"Необработанная ошибка при получении списка ID по компаниям: {e}")
        return None


def split_list(list_: list, size: int) -> list:
    """
    Разделяет список на подсписки заданного размера.

    Аргументы:
        list_: Список, который нужно разделить.
        size: Размер каждого подсписка.

    Возвращает:
        Список подсписков, где каждый подсписок имеет максимальный размер `size`.
    """
    return [list_[i:i + size] for i in range(0, len(list_), size)]


def get_uuid(token: str, date_from: str, date_to: str, campaigns_id: list) -> str | None:
    """
    Получает UUID, используя указанный токен, диапазон дат и необязательный идентификатор кампании.

    Аргументы:
        token (str): Токен, используемый для аутентификации.
        date_from (str): Начальная дата диапазона в формате "гггг-мм-дд".
        date_to (str): Конечная дата диапазона в формате "гггг-мм-дд".
        campaigns_id (list): Идентификатор кампании. Список идентификаторов может быть пустым.

    Возвращает:
        str: UUID, полученный из ответа API.

    Выбрасывает:
        requests.RequestException: Если произошла ошибка в запросе.
        Exception: Если произошла необработанная ошибка.
    """
    try:
        if len(campaigns_id) == 0:
            return None
        params = get_uuid_params(token, date_from, date_to, campaigns_id)

        response = requests.post(url=params["url"], headers=params["headers"], json=params["params"])
        response.raise_for_status()

        if response.status_code == 200:
            return response.json()["UUID"]
        logging.error(f"Ошибка получения UUID: {response.text}")
        return None

    except requests.RequestException as req_err:
        logging.error(f"Ошибка запроса при получении UUID: {response.text} * {response.status_code} * {req_err}")
        return None

    except Exception as e:
        logging.error(f"Необработанная ошибка при получении UUID: {e}")
        return None


def check_uuid_status(uuid: str, token: str) -> str:
    """
    Проверяет статус UUID.

    Аргументы:
        uuid (str): UUID для проверки.
        token (str): Токен авторизации.

    Возвращает:
        tuple: Кортеж, содержащий булевое значение, указывающее на статус, и строку с сообщением об ошибке.

    Вызывает:
        requests.RequestException: Если произошла ошибка с HTTP-запросом.
        Exception: Если произошла необработанная ошибка.
    """
    try:
        params = get_check_uuid_status_params(token, uuid)

        response = requests.get(url=params['url'], headers=params['headers'])
        response.raise_for_status()  # Проверяем успешность запроса, поднимаем исключение при ошибке HTTP

        if response.status_code == 200:
            json_response = response.json()
            if json_response["state"] == "OK":
                return "OK"
            elif json_response["state"] == "ERROR":
                logging.error(f"Ошибка в запросе статуса : {json_response}")
                return "ERROR"
            elif json_response['state'] == 'IN_PROGRESS':
                return "IN_PROGRESS"
        else:
            logging.error(f"Ответ при проверке статуса UUID {response.status_code}: с текстом {response.text}")
            return "STATUS_ERROR"
    except requests.RequestException as req_err:
        logging.error(f"Ошибка запроса при проверке статуса UUID: {req_err}")
        return f"{req_err}"
    except Exception as e:
        logging.error(f"Необработанная ошибка при проверке статуса UUID: {e}")
        return f"{e}"


def ensure_directory_exists(directory_path):
    """
         Проверяет, что указанный каталог существует. Если каталог уже существует,
         возвращает False. Если каталог не существует, создает его и возвращает True.

         Параметры:
         - путь_каталога (str): путь к каталогу, существование которого необходимо проверить.

         Возврат:
         - bool: True, если каталог был создан, в противном случае — False.
         """
    try:
        if os.path.exists(directory_path):
            return False
        else:
            os.makedirs(directory_path)
            return True
    except Exception as e:
        logging.error(f"Не удалось создать директорию по маршруту {directory_path}: {e}")
        return False


def download_file(token: str, uuid: str, file_path: str) -> tuple:
    try:
        params = get_download_file_params(token, uuid)

        response = requests.get(url=params['url'], headers=params['headers'], stream=True)
        response.raise_for_status()  # Проверяем успешность запроса, поднимаем исключение при ошибке HTTP

        if response.status_code == 200:
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            logging.info(f"Файл успешно скачан по пути: {file_path}")
            return True, ""
        else:
            logging.error(f"Не удалось скачать файл. Код статуса: {response.status_code}, Текст ошибки {response.text}")
            return False, response.text
    except requests.RequestException as req_err:
        logging.error(f"Ошибка запроса при скачивании файла: {response.text} * {response.status_code} * {req_err}")
        return False, req_err
    except Exception as e:
        logging.error(f"Необработанная ошибка при скачивании файла: {e}")
        return False, e


def clear_folder(folder_path):
    """
    Очищает указанную папку, удаляя все файлы и подпапки внутри нее.

    Аргументы:
        folder_path (str): Путь к папке, которую необходимо очистить.

    Исключения:
        OSError: Если происходит ошибка при удалении файла или папки.

    Возвращает:
        None
    """
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.error(f"Не удалось удалить файл {file_path}: {e}")


def delete_directory(directory_path):
    try:
        shutil.rmtree(directory_path)
        return True
    except Exception as e:
        logging.error(f"Ошибка при удалении директории {directory_path}: {e}")
        return False
