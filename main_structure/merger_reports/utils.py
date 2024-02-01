import logging
import requests
import os

from main_structure.merger_reports.config import get_analytics_params

logging.basicConfig(filename='app.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    encoding='utf-8')


def get_analytics(date_from, date_to, seller_id, seller_api_key):
    try:
        params = get_analytics_params(date_from, date_to, seller_id, seller_api_key)
        response = requests.post(url=params["url"], headers=params["headers"], json=params["body"])

        response.raise_for_status()  # Проверяем успешность запроса, поднимаем исключение при ошибке HTTP

        if response.status_code == 200:
            data = response.json()
            return data["result"]["data"]
        else:
            logging.error(f"Ошибка при запросе данных. Код статуса: {response.status_code}")
            return None
    except requests.RequestException as req_err:
        logging.error(f"Ошибка при запросе данных: {req_err}")
        return None
    except Exception as e:
        logging.error(f"Необработанная ошибка: {e}")
        return None


def ensure_directory_exists(directory_path):
    try:
        if os.path.exists(directory_path):
            return False
        else:
            os.makedirs(directory_path)
            return True
    except Exception as e:
        logging.error(f"Не удалось создать директорию по маршруту {directory_path}: {e}")
        return False
