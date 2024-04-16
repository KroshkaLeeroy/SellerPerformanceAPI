import pandas as pn
from main_structure.new.utils import *


class Reader:
    def __init__(self, client_folder, date):
        self.csv_folder = os.path.join('downloads', client_folder, date, 'csv_folder')
        self.promo_folder = os.path.join(self.csv_folder, 'PROMO')
        self.sku_folder = os.path.join(self.csv_folder, 'SKU')

        self.inf_folder_stat = os.path.join('downloads', client_folder, date, 'stat', 'reader')

        self.inf_initial_data = {
            'client_folder': client_folder,
            'date': date,
            'csv_folder': ensure_directory_exists(self.csv_folder),
            'promo_folder': ensure_directory_exists(self.promo_folder),
            'sku_folder': ensure_directory_exists(self.sku_folder),
            'inf_folder_stat': ensure_directory_exists(self.inf_folder_stat),
        }
        self.inf_sku_process = {}
        self.inf_promo_process = {}
        self.inf_total = {
            'success': False,
            'inf_initial_data': self.inf_initial_data,
            'inf_sku_process': self.inf_sku_process,
            'inf_promo_process': self.inf_promo_process,
        }

        write_json(self.inf_initial_data, self.inf_folder_stat + '\\1_initial.json')

    def run(self):
        sku = self.read_files_sku()
        promo = self.read_files_promo()

        self.inf_total = {
            'success': all((self.inf_sku_process['success'], self.inf_promo_process['success'])),
            'inf_initial_data': self.inf_initial_data,
            'inf_sku_process': self.inf_sku_process,
            'inf_promo_process': self.inf_promo_process,
        }

        write_json(self.inf_total, self.inf_folder_stat + '\\main.json')

        return sku, promo

    def read_files_promo(self):
        get_list_names = search_files(self.promo_folder)

        self.inf_promo_process = {
            'all_files_names': get_list_names,
            'read_history': [],
            'success': False,
            'text_error': ''
        }

        merge_data = {
            'correction': 0
        }

        for index, name in enumerate(get_list_names):
            try:
                status, info = self.get_data_from_promo_file(index, name, merge_data)
                info['companies_in_file'] = len(info["goods_ids"])
                self.inf_promo_process['read_history'].append(info)
            except Exception as e:
                print(str(type(e).__name__), e)
                info = {
                    'number': index,
                    'success': False,
                    'filename': name,
                    'text_error': (str(type(e).__name__), str(e))
                }
                self.inf_promo_process['read_history'].append(info)

        self.inf_promo_process['success'] = all(
            [success['success'] for success in self.inf_promo_process['read_history']])

        write_json(self.inf_promo_process, self.inf_folder_stat + '\\3_read_promo.json')

        return merge_data

    def read_files_sku(self):
        get_list_names = search_files(self.sku_folder)

        self.inf_sku_process = {
            'all_files_names': get_list_names,
            'read_history': [],
            'success': False,
            'text_error': ''
        }

        merge_data = {
            'correction': 0
        }

        for index, name in enumerate(get_list_names):
            try:
                status, info = self.get_data_from_sku_file(index, name, merge_data)
                info['companies_in_file'] = len(info["goods_ids"])
                self.inf_sku_process['read_history'].append(info)
            except Exception as e:
                print((str(type(e).__name__), str(e)))
                info = {
                    'number': index,
                    'success': False,
                    'filename': name,
                    'text_error': str(type(e).__name__, str(e))
                }
                self.inf_sku_process['read_history'].append(info)

        self.inf_sku_process['success'] = all([success['success'] for success in self.inf_sku_process['read_history']])

        write_json(self.inf_sku_process, self.inf_folder_stat + '\\2_read_sku.json')

        return merge_data

    def get_data_from_sku_file(self, index, name, merge_data):
        file_data = {
            'number': index,
            'filename': name,
            'goods_ids': [],
            'file_contains_data': False,
            'companies_in_file': 0,
            'correction_in_file': False,
            'success': False,
            'text_error': ''
        }

        path = os.path.join(self.sku_folder, name)
        try:
            info = pn.read_csv(path, sep=";", skiprows=[0])
        except Exception as e:
            print((str(type(e).__name__), str(e)))
            file_data['text_error'] = str(type(e).__name__, str(e))
            return False, file_data

        if len(info) > 1:
            file_data['file_contains_data'] = True
            data_from_csv = info.to_dict()
            for key, company in data_from_csv['sku'].items():
                if company.isnumeric():
                    file_data['goods_ids'].append(company)
                    if company not in merge_data:
                        merge_data[company] = {'Показы': data_from_csv['Показы'][key],
                                               'Клики': data_from_csv['Клики'][key],
                                               'Расход': float(
                                                   data_from_csv['Расход, ₽, с НДС'][key].replace(',', '.'))}
                    else:
                        merge_data[company]['Показы'] += data_from_csv['Показы'][key]
                        merge_data[company]['Показы'] += data_from_csv['Клики'][key]
                        merge_data[company]['Показы'] += float(
                            data_from_csv['Расход, ₽, с НДС'][key].replace(',', '.'))
                elif company == 'Корректировка':
                    file_data['correction_in_file'] = True
                    merge_data['correction'] += float(data_from_csv['Расход, ₽, с НДС'][key].replace(',', '.'))
        file_data['success'] = True
        return True, file_data

    def get_data_from_promo_file(self, index, name, merge_data):
        file_data = {
            'number': index,
            'filename': name,
            'goods_ids': [],
            'file_contains_data': False,
            'companies_in_file': 0,
            'correction_in_file': False,
            'success': False,
            'text_error': ''
        }

        path = os.path.join(self.promo_folder, name)
        try:
            info = pn.read_csv(path, sep=";", skiprows=[0])
        except Exception as e:
            print((str(type(e).__name__), str(e)))
            file_data['text_error'] = str(type(e).__name__, str(e))
            return False, file_data

        data_from_csv = info.to_dict()
        if len(data_from_csv['Ozon ID']) > 1:
            file_data['file_contains_data'] = True
            for key, company in data_from_csv['Ozon ID'].items():
                if not pn.isna(company):
                    company = str(int(company))
                    file_data['goods_ids'].append(company)
                    if company not in merge_data:
                        merge_data[company] = {
                            'Расход': float(data_from_csv['Расход, ₽'][key].replace(',', '.')),
                        }
                    else:
                        merge_data[company]['Расход'] += float(
                            data_from_csv['Расход, ₽'][key].replace(',', '.'))
                if data_from_csv['Дата'][key] == 'Корректировка':
                    file_data['correction_in_file'] = True
                    merge_data['correction'] += float(data_from_csv['Расход, ₽'][key].replace(',', '.'))
        file_data['success'] = True
        return True, file_data
