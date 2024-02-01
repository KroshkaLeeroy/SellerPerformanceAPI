import os.path
from collections import defaultdict
import pandas as pd

from .utils import get_analytics, logging, ensure_directory_exists
from ..download_folder.download_data import DownloadZip
from ..read_zip_folder.read_data import ReadAllFromZip

PATH_TO_REPORTS = "reports"


class MergerReports:
    def __init__(self, date_from, date_to, perf_id, perf_secret, seller_id, seller_api_key, client_folder, debug=False):
        self.date_from = date_from
        self.date_to = date_to

        self.seller_id = seller_id
        self.seller_api_key = seller_api_key

        self.client_folder = client_folder

        self.total_exel_path = os.path.join(PATH_TO_REPORTS, self.client_folder,
                                            f"{self.date_from}_{self.date_to}.xlsx")

        self.debug = debug

        self.download_PROMO = DownloadZip(date_from, date_to, 'PROMO', perf_id, perf_secret, client_folder, debug=debug)
        self.download_SKU = DownloadZip(date_from, date_to, 'SKU', perf_id, perf_secret, client_folder, debug=debug)

        self.read_PROMO = ReadAllFromZip(date_from, date_to, 'PROMO', client_folder, debug=debug)
        self.read_SKU = ReadAllFromZip(date_from, date_to, 'SKU', client_folder, debug=debug)

        self.query = f'Клиент: {self.client_folder} Отчет: {self.date_from}_{self.date_to}'

        self.missed_companies = {
            'SKU': [],
            "PROMO": []
        }
        self.any_errors = False

        self.query_counter = 0

    def debug_(self, text):
        if self.debug:
            print(text)

    def start_download(self):
        self.download_PROMO.download_files()
        logging.warning(f"Загрузка PROMO завершена пропущены ID компаний {self.download_PROMO.UUID_error_request_set}")
        self.missed_companies.get('PROMO').append(self.download_PROMO.UUID_error_request_set)

        self.download_SKU.download_files()
        logging.warning(f"Загрузка SKU завершена пропущены ID компаний {self.download_SKU.UUID_error_request_set}")
        self.missed_companies.get('SKU').append(self.download_SKU.UUID_error_request_set)

        if self.download_PROMO.any_errors or self.download_SKU.any_errors:
            self.any_errors = True

        self.query_counter = self.download_PROMO.query_counter + self.download_SKU.query_counter

        return self.any_errors

    def merge_reports(self):
        list_1, correct1 = self.read_PROMO.read_files()
        list_2, correct2 = self.read_SKU.read_files()
        correct1 += correct2
        analytics = self.get_analytics_data()
        total_list = self.get_total_statistic(analytics, list_2, list_1)

        if total_list is None:
            self.any_errors = True
            return self.any_errors

        if self.read_PROMO.any_errors or self.read_SKU.any_errors:
            self.any_errors = True

        print('Promo', self.read_PROMO.any_errors, 'SKU', self.read_SKU.any_errors)

        total_list.append([correct1])

        status = self.create_exel_report(total_list)
        if not status:
            self.any_errors = True

        return self.any_errors

    def create_exel_report(self, total_list):
        try:
            file_name = os.path.join(PATH_TO_REPORTS, self.client_folder, f"{self.date_from}_{self.date_to}.xlsx")

            ensure_directory_exists(os.path.join(PATH_TO_REPORTS, self.client_folder))

            dt = pd.DataFrame(total_list,
                              columns=['Наименование', "Ozon ID", "Заказано товаров", "Заказано на сумму", "Показы",
                                       "Клики", "Стоимость", "ДРР"])
            dt.to_excel(file_name, index=False)
            return True
        except Exception as e:
            error_msg = f"Ошибка при создании exel отчета {self.query}: {e}"
            logging.error(error_msg)
            self.debug_(error_msg)
            return False

    def get_analytics_data(self):
        result = get_analytics(self.date_from, self.date_to, self.seller_id, self.seller_api_key)

        if not result:
            error_msg = f"Не удалось получить аналитику клиента: {self.query}."
            logging.error(error_msg)
            self.debug_(error_msg)
            return None

        message = f"Данные с запроса аналитики клиента получены {self.query}."
        logging.info(message)
        self.debug_(message)

        try:
            analytics_data = {}

            for info in result:
                try:
                    id = info["dimensions"][0]["id"]
                    name = info["dimensions"][0]["name"]
                    date = info["dimensions"][1]["id"]
                    ordered_units = int(info["metrics"][1])
                    revenue = int(info["metrics"][0])

                    if id not in analytics_data:
                        analytics_data[id] = [id, name, date, ordered_units, revenue]
                    else:
                        analytics_data[id][3] += ordered_units
                        analytics_data[id][4] += revenue
                except (KeyError, ValueError) as e:
                    error_msg = f"Ошибка при обработке данных аналитики клиента {self.query}: {e}: {info}"
                    logging.error(error_msg)
                    self.debug_(error_msg)
                    return None

            ready_result = list(analytics_data.values())
            ready_result.extend([["" for _ in range(5)] for _ in range(100)])

            return ready_result
        except Exception as e:
            error_msg = f"По клиенту {self.query} вовремя получения аналитики. Необработанная ошибка: {e}"
            logging.error(error_msg)
            self.debug_(error_msg)
            return None

    def processing_analytics_data(self, dict_data, analytics):
        try:
            for item in analytics:
                if item[0] != "":
                    id_ = item[0]
                    dict_data[id_].update({
                        "name": item[1],
                        "id": id_,
                        "ordered_units": item[3],
                        "revenue": item[4],
                    })
            return dict_data
        except Exception as e:
            error_msg = f"Ошибка при обработке данных в process_analytics_data {self.query}: {e}"
            logging.error(error_msg)
            self.debug_(error_msg)
            return None

    def processing_sku_data(self, dict_data, list_SKU):
        try:
            for item in list_SKU:
                id = item[0]
                dict_data[id]["views"] += int(item[-9])
                dict_data[id]["clicks"] += int(item[-8])
                dict_data[id]["count"] += float(item[-5])

            return dict_data
        except Exception as e:
            error_msg = f"Ошибка при обработке данных в process_sku_data {self.query}: {e}"
            logging.error(error_msg)
            self.debug_(error_msg)
            return None

    def processing_promo_data(self, dict_data, list_PROMO):
        try:
            for item in list_PROMO:
                id = item[3]
                dict_data[id]["count"] += float(item[-1])
            return dict_data
        except Exception as e:
            error_msg = f"Ошибка при обработке данных в process_promo_data {self.query}: {e}"
            logging.error(error_msg)
            self.debug_(error_msg)
            return None

    def merge_all_to_list(self, total_dict):
        try:
            total_list = []
            sum_count = 0
            for item in total_dict.values():
                if item["count"] != 0 and item["revenue"]:
                    drr = round((item["count"] / item["revenue"]) * 100, 2)
                else:
                    drr = 0

                temp = [
                    item["name"],
                    item["id"],
                    item["ordered_units"],
                    item["revenue"],
                    item["views"],
                    item["clicks"],
                    item["count"],
                    drr
                ]

                sum_count += item["count"]
                total_list.append(temp)

            total_list.extend([["" for _ in range(len(total_list[0]))] for _ in range(200)])
            return total_list
        except Exception as e:
            error_msg = f"Ошибка при объединении данных в merge_all_to_list {self.query}: {e}"
            logging.error(error_msg)
            self.debug_(error_msg)
            return None

    def get_total_statistic(self, analytics, list_SKU, list_PROMO):
        total_dict = defaultdict(
            lambda: {"name": "", "id": "", "ordered_units": 0, "revenue": 0, "views": 0, "clicks": 0, "count": 0})

        total_dict = self.processing_analytics_data(total_dict, analytics)
        self.check_if_all_good('Analytics', total_dict)

        total_dict = self.processing_sku_data(total_dict, list_SKU)
        self.check_if_all_good('SKU', total_dict)

        total_dict = self.processing_promo_data(total_dict, list_PROMO)
        self.check_if_all_good('PROMO', total_dict)

        total_list = self.merge_all_to_list(total_dict)
        self.check_if_all_good('Отчетов', total_list)

        return total_list

    def check_if_all_good(self, place, data):
        if data is None:
            self.any_errors = False
            er_message = f'Что то пошло не так при финальной обработке {place} {self.query}'
            logging.warning(er_message)
