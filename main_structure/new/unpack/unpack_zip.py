import os.path
import shutil
from main_structure.new.utils import *


class ZipUnpack:
    def __init__(self, client_folder, date):
        self.client_folder = client_folder
        self.date = date

        self.zip_folder = os.path.join('downloads', client_folder, date)
        self.csv_folder = os.path.join(self.zip_folder, 'csv_folder')

        self.inf_folder_stat = os.path.join(self.zip_folder, 'stat', 'zip_unpack')

        self.inf_initial_data = {
            'success': True,
            'client_folder': self.client_folder,
            'date': date,
            'zip_folder': self.zip_folder,
            'csv_folder': self.csv_folder,
            'csv_folder_create': ensure_directory_exists(self.csv_folder),
            'inf_folder_stat': self.inf_folder_stat,
            'folder_create': ensure_directory_exists(self.inf_folder_stat),
        }

        write_json(self.inf_initial_data, os.path.join(self.inf_folder_stat, '1_initial.json'))

        self.inf_open_zip = {
            'success': False,
            'SKU': {
                'success': False,
                'counter_zip': 0,
                'counter_csv': 0,
                'history': [],
            },
            'PROMO': {
                'success': False,
                'counter_zip': 0,
                'counter_csv': 0,
                'history': [],
            },
            'time': {},
        }

        self.inf_total_report = {
            'success': False,
            'success_epoch': {
                'initial_data': False,
                'open_zip': False,
            },
            'inf_initial_data': self.inf_initial_data,
            'inf_open_zip': self.inf_open_zip,
            'time': 0,
        }

    @time_decorator
    def open_all_zip(self):
        try:
            for file in self.inf_initial_data['folder_files_path']:
                for type_ in ['PROMO', 'SKU']:
                    type_path = os.path.join(self.csv_folder, type_)
                    temp = {
                        'success': False,
                        'text_error': '',
                        'zip_path': file,
                        'files_names': [],
                        'create_unpack_folder': ensure_directory_exists(type_path),
                        'unpack_path': type_path,
                        'copy': False,
                    }
                    if file.endswith('.zip'):
                        if type_ in file:
                            result, time_info = unpack_zip(file, type_path)
                            status, value = result
                            temp.update(time_info)
                            if status:
                                temp['success'] = True
                                temp['files_names'] = value
                                self.inf_open_zip[type_]['counter_csv'] += len(value)
                            else:
                                temp['text_error'] = value
                            self.inf_open_zip[type_]['history'].append(temp)
                            self.inf_open_zip[type_]['counter_zip'] += 1

                    elif file.endswith('.csv'):
                        if type_ in file:
                            shutil.copy(file, type_path)
                            temp['success'] = True
                            temp['copy'] = True
                            self.inf_open_zip[type_]['history'].append(temp)
                            self.inf_open_zip[type_]['counter_zip'] += 1
                            self.inf_open_zip[type_]['counter_csv'] += 1
                write_json(self.inf_open_zip, os.path.join(self.inf_folder_stat, '2_open_zip.json'))
            sku_dict = self.inf_open_zip['SKU']
            promo_dict = self.inf_open_zip['PROMO']

            sku_counter = sum(d.get('success', False) for d in sku_dict['history'])
            promo_counter = sum(d.get('success', False) for d in promo_dict['history'])

            if sku_counter == sku_dict['counter_zip'] and promo_counter == promo_dict['counter_zip']:
                return True, ''
            text = f'Successful executions (SKU {sku_counter}, PROMO {promo_counter}) \n' + \
                   f'unzipped zip (SKU {sku_dict["counter_zip"]}, PROMO {promo_dict["counter_zip"]})'
            return False, text
        except Exception as e:
            print((str(type(e).__name__), str(e), 'Error while unpack'))
            return False, (str(type(e).__name__), str(e))

    def run(self):
        self.files_names = search_files(self.zip_folder)
        self.inf_initial_data.update({
            'folder_files': self.files_names,
            'folder_files_path': [os.path.join(self.zip_folder, name) for name in self.files_names]
        })

        self.inf_open_zip['files_counter'] = len(self.inf_initial_data['folder_files_path'])
        result, time_info = self.open_all_zip()
        status, value = result
        self.inf_open_zip['time'] = time_info
        self.inf_open_zip['success'] = status
        self.inf_open_zip['error_text'] = value
        write_json(self.inf_open_zip, os.path.join(self.inf_folder_stat, '2_open_zip.json'))

        self.inf_total_report['success_epoch']['initial_data'] = self.inf_initial_data['success']
        self.inf_total_report['success_epoch']['open_zip'] = self.inf_open_zip['success']

        status_1 = self.inf_total_report['success_epoch']['initial_data']
        status_2 = self.inf_total_report['success_epoch']['open_zip']
        self.inf_total_report['success'] = all((status_1, status_2))

        write_json(self.inf_total_report, os.path.join(self.inf_folder_stat, 'main.json'))
