import os
import zipfile

import logging

# Настройка логирования

logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_file_names(folder_path):
    return [file_name for file_name in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file_name))]


def read_zip(zip_file_path):
    list_zip = []
    correct_zip = []
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith(".csv"):
                    list_csv = []
                    correct_csv = []
                    with zip_ref.open(file_name, mode="r") as csv_file:
                        csv_content = csv_file.read().decode("utf-8")
                        campaign = csv_content.split("\n")[2:-2]
                        if campaign:
                            list_csv = [temp.split(";") for temp in campaign if "Корректировка" not in temp]
                            correct_csv = [temp for temp in campaign if "Корректировка" in temp]
                    list_zip += list_csv
                    correct_zip += correct_csv

        return list_zip, correct_zip
    except zipfile.BadZipFile as bad_zip_err:
        logging.error(f"Ошибка при чтении ZIP-файла: {bad_zip_err}")
        return [], []
    except Exception as e:
        logging.error(f"Необработанная ошибка: {e}")
        return [], []


def convert_from_zip_to_list(file_name):
    try:
        data, correct_zip = read_zip(file_name)

        if not data:
            logging.info(f"Нет данных в ZIP-файле {file_name} ")
        else:
            logging.info(f"Успешно прочитан ZIP-файл: {file_name}")

        exit_data = [[item for item in info if item != ""] for info in data]
        correct = [float(item.split(";")[-1].replace(",", ".")) for item in correct_zip]

        return exit_data, correct
    except Exception as e:
        logging.error(f"Ошибка при конвертации данных из ZIP-файла: {e}")
        return [], []


def convert_from_string_to_int(list_items, index):
    try:
        temp = [float(item.replace(",", ".")) for item in list_items[-index:]]
        list_items = list_items[:-index] + temp
        return list_items
    except ValueError as value_err:
        logging.error(f"Ошибка при конвертации строки в число: {value_err}")
        return list_items
    except Exception as e:
        logging.error(f"Необработанная ошибка: {e}")
        return list_items
