import time
import json
import os
import zipfile


def time_decorator(func):
    def wrapper(*args, **kwargs):
        time_start = time.time()
        result = func(*args, **kwargs)  # Вызов функции
        time_end = time.time()
        time_info = round(time_end - time_start, 3)
        print(f"Execution time {func.__name__}: {time_info} sec")
        return result, {'time': time_info}

    return wrapper


def write_json(data, path, work_type='w'):
    with open(path, work_type, encoding='utf-8') as file:
        json.dump(data, file)


def read_json(path):
    with open(path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        return data


def ensure_directory_exists(directory_path):
    if os.path.exists(directory_path):
        return False
    else:
        os.makedirs(directory_path)
        return True


def search_files(path):
    return os.listdir(path)


@time_decorator
def unpack_zip(zip_file, folder):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(folder)
        return True, zip_ref.namelist()
    except Exception as e:
        print((str(type(e).__name__), str(e), 'Error while unpack'))
        return False, str(str(type(e).__name__), str(e))
