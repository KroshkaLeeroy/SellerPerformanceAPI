from __future__ import annotations

import shutil
import traceback
from typing import Tuple, Any

import requests
from main_structure.new.utils import *
from main_structure.new.downloader.config import *


@time_decorator
def get_token(client_id: str, client_secret: str) -> tuple[bool, Any] | tuple[bool, str]:
    get_dict = get_token_params(client_id, client_secret)

    url = get_dict.get("url")
    headers = get_dict.get("headers")
    params = get_dict.get("params")

    data = json.dumps(params)
    result = requests.post(url=url, headers=headers, data=data)
    try:
        token = result.json().get('access_token')
        return True, token
    except Exception as e:
        print((str(type(e).__name__), str(e), traceback.format_exc(), result.text))
        return False, f'{type(e).__name__}, {str(e)}, {traceback.format_exc()}, {result.text}'


@time_decorator
def get_list_of_ids(token: str) -> None | Tuple:
    params = get_list_of_id_params(token)
    response = requests.get(url=params["url"], headers=params["headers"], json=params["params"])
    try:
        json_dict = response.json()["list"]
        sku = [campaign["id"] for campaign in json_dict if campaign["advObjectType"] == "SKU"]
        search_promo = [campaign["id"] for campaign in json_dict if campaign["advObjectType"] == "SEARCH_PROMO"]
        return True, sku, search_promo
    except Exception as e:
        print((str(type(e).__name__), str(e), traceback.format_exc(), response.text))
        return False, f'{type(e).__name__}, {str(e)}', f'{traceback.format_exc()}, {response.text}'


@time_decorator
def split_list(list_: list, size: int) -> tuple:
    try:
        return True, [list_[i:i + size] for i in range(0, len(list_), size)]
    except Exception as e:
        print((str(type(e).__name__), str(e)))
        return False, (str(type(e).__name__), str(e))


@time_decorator
def get_uuid_count(token: str, date_from: str, date_to: str, campaigns_id: list) -> Tuple | None:
    if len(campaigns_id) == 0:
        return False, 'Received 0 companies'
    params = get_uuid_params(token, date_from, date_to, campaigns_id)
    response = requests.post(url=params["url"], headers=params["headers"], json=params["params"])

    try:
        return True, response.json()["UUID"]
    except Exception as e:
        print((str(type(e).__name__), str(e), traceback.format_exc(), response.text))
        return False, (str(type(e).__name__), str(e), traceback.format_exc(), response.text)


@time_decorator
def check_uuid_status_count(uuid: str, token: str) -> str | tuple[str, str] | Any:
    params = get_check_uuid_status_params(token, uuid)
    response = requests.get(url=params['url'], headers=params['headers'])
    try:
        json_response = response.json()
        if json_response["state"] == "OK":
            return "OK"
        elif json_response["state"] == "ERROR":
            return json_response
        elif json_response['state'] == 'IN_PROGRESS':
            return "IN_PROGRESS"
        elif json_response['state'] == 'NOT_STARTED':
            return "IN_PROGRESS"
        else:
            return json_response
    except Exception as e:
        print(str(type(e).__name__), str(e), traceback.format_exc(), response.text)
        return str(type(e).__name__), str(e), traceback.format_exc(), response.text


def ensure_directory_exists(directory_path):
    if os.path.exists(directory_path):
        return False
    else:
        os.makedirs(directory_path)
        return True


@time_decorator
def download_file_count(token: str, uuid: str, file_path: str) -> tuple:
    params = get_download_file_params(token, uuid)
    try:
        response = requests.get(url=params['url'], headers=params['headers'], stream=True)
        if response.status_code == 200:
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            return True, "Successfully downloaded"
        else:
            return False, f'Unknowing error while downloading report {file_path}'
    except Exception as e:
        print((str(type(e).__name__), str(e), f'Unknowing error while downloading report {file_path}'))
        return False, (str(type(e).__name__), str(e), f'Unknowing error while downloading report {file_path}')


def clear_folder(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


def delete_directory(directory_path):
    shutil.rmtree(directory_path)
    return True
