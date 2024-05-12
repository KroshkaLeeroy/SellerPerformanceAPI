import os.path
import time

import pandas as pn
from main_structure.new.unpack.unpack_zip import ZipUnpack
from main_structure.new.reader.reader import Reader
from main_structure.new.downloader.downloader import DownloadZip
from main_structure.new.merger.utils import get_analytics
from main_structure.new.utils import ensure_directory_exists, write_json
import traceback
import xlsxwriter


class Merger:
    def __init__(self, date_from, date_to, perf_id, perf_secret, seller_id, seller_api_key, client_folder):
        self.date_from = date_from
        self.date_to = date_to
        self.perf_id = perf_id
        self.perf_secret = perf_secret
        self.seller_id = seller_id
        self.seller_api_key = seller_api_key
        self.client_folder = client_folder
        self.date = self.date_from + '_' + self.date_to
        self.stats_folder_path = os.path.join('downloads', self.client_folder, self.date, 'stat', 'merge')
        self.client_folder_path = os.path.join('downloads', self.client_folder)
        self.client_report_path = os.path.join('downloads', self.client_folder, self.date, self.date) + '.xlsx'

        self.downloader = DownloadZip(self.date_from, self.date_to, self.perf_id, self.perf_secret, self.client_folder)
        self.unpacker = ZipUnpack(self.client_folder, self.date)
        self.reader = Reader(self.client_folder, self.date)

        self.inf_initial_data = {
            'date_from': self.date_from,
            'date_to': self.date_to,
            'perf_id': self.perf_id,
            'perf_secret': self.perf_secret,
            'seller_id': self.seller_id,
            'seller_api_key': self.seller_api_key,
            'client_folder_name': self.client_folder,
            'client_folder_path': self.client_folder_path,
            'client_folder_exist': ensure_directory_exists(self.client_folder_path),
            'stats_folder_path': self.stats_folder_path,
            'stats_folder_exist': ensure_directory_exists(self.stats_folder_path),
            'client_report_path': self.client_report_path,
            'date': self.date,
        }
        write_json(self.inf_initial_data, os.path.join(self.stats_folder_path, '1_initial.json'))

        self.inf_transform_data = {}

        self.inf_total_join = {}

    def run(self):
        self.downloader.run()
        self.unpacker.run()
        sku, promo = self.reader.run()
        self.create_report(sku, promo)

    def create_report(self, sku, promo):
        status, info = get_analytics(self.date_from, self.date_to, self.seller_id, self.seller_api_key)

        self.inf_transform_data = {
            'get_reports_success': status,
            'join_reports_success': False,
            'create_report_success': False,
        }

        info = self.transform(info)
        write_json(self.inf_transform_data, os.path.join(self.stats_folder_path, '2_transform.json'))

        total_dict = self.join_analytics(info, sku, promo)
        self.inf_transform_data['join_reports_success'] = True

        write_json(self.inf_total_join, os.path.join(self.stats_folder_path, '3_join.json'))

        self.create_endless_report(total_dict)
        self.inf_transform_data['create_report_success'] = True

        write_json(self.inf_transform_data, os.path.join(self.stats_folder_path, 'main.json'))

    def create_endless_report(self, data_list):
        try:
            workbook = xlsxwriter.Workbook(self.client_report_path)
            worksheet = workbook.add_worksheet('Товары')

            title_list = ['Наименование', 'Ozon ID', 'Показы', 'CTR',
                          'Клики', 'Заказано товаров', 'Заказано на сумму',
                          'Трафареты', 'Продвижение в поиске', 'Ср. цена продажи', 'ДРР\n%', 'Комментарий']

            title_cell_format = workbook.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vcenter',
                'pattern': 1,
                'font_color': 'ffffff',
                'bg_color': '#1a42f5',
                'text_wrap': True,
                'border_color': 'ffffff',
                'border': True,
            })

            procent_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
            })
            DRR_column_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'border_color': 'ffffff',
                'border': True,
            })
            number_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
            })
            value_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'num_format': '_-* # ##0 ₽_-;-* # ##0 ₽_-;_-* "-" ₽_-;_-@_-'
            })
            name_cell_format = workbook.add_format({
                'align': 'left',
                'valign': 'vcenter',
                'text_wrap': True,
            })
            summary_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'num_format': '#,##0.00'
            })

            DRR_cell_format = workbook.add_format({
                'bold': True,
                'align': 'center',
                'valign': 'vcenter',
                'num_format': '#,##0.00',
                'border_color': 'ffffff',
            })

            conditional_1_DRR_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#D9D9D9',
                'num_format': '#,##0.00'
            })
            conditional_2_DRR_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#84CD73',
                'num_format': '#,##0.00'
            })
            conditional_3_DRR_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#F7EB7B',
                'num_format': '#,##0.00'
            })
            conditional_4_DRR_cell_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'bg_color': '#E25B45',
                'num_format': '#,##0.00'
            })

            pixels = [500, 100, 80, 50, 60, 70, 95, 95, 105, 80, 70, 500]

            formulas = [
                ['C1', '=SUM(C3:C100000)', number_cell_format],
                ['D1', '=AVERAGE(D3:D100000)', summary_cell_format],
                ['E1', '=SUM(E3:E100000)', number_cell_format],
                ['F1', '=SUM(F3:F100000)', number_cell_format],
                ['G1', '=SUM(G3:G100000)', value_cell_format],
                ['H1', '=SUM(H3:H100000)', value_cell_format],
                ['I1', '=SUM(I3:I100000)', value_cell_format],
                ['J1', '=AVERAGE(J3:J100000)', value_cell_format],
                ['K1', '=(H1+I1)/G1*100', DRR_cell_format],
            ]

            for column, formula, format in formulas:
                worksheet.write(column, formula, format)

            for index, title_column in enumerate(title_list):
                worksheet.write(1, index, title_column, title_cell_format)
                worksheet.set_column_pixels(index, index, pixels[index])

            worksheet.set_row_pixels(1, 75)
            worksheet.autofilter('A2:K100000')
            worksheet.freeze_panes(2, 0)

            worksheet.conditional_format('K3:K10000',
                                         {'type': 'cell', 'criteria': '=', 'value': 0,
                                          'format': conditional_1_DRR_cell_format})
            worksheet.conditional_format('K3:K10000',
                                         {'type': 'cell', 'criteria': '<=', 'value': 20,
                                          'format': conditional_2_DRR_cell_format})
            worksheet.conditional_format('K3:K10000',
                                         {'type': 'cell', 'criteria': '<', 'value': 30,
                                          'format': conditional_3_DRR_cell_format})
            worksheet.conditional_format('K3:K10000',
                                         {'type': 'cell', 'criteria': '>=', 'value': 30,
                                          'format': conditional_4_DRR_cell_format})

            for row_num, text_row in enumerate(data_list, 2):
                text_row['comment'] = ''
                for column_num, text_column in enumerate(text_row, ):
                    style = {
                        'name': name_cell_format,
                        'id': number_cell_format,
                        'views': number_cell_format,
                        'CTR': procent_cell_format,
                        'clicks': number_cell_format,
                        'ordered_units': number_cell_format,
                        'revenue': value_cell_format,
                        'SKU_count': value_cell_format,
                        'PROMO_count': value_cell_format,
                        'avg_sell': value_cell_format,
                        'DRR': DRR_column_cell_format,
                        'comment': name_cell_format,
                    }
                    if text_column == 'CTR' or text_column == 'DRR':
                        text_row[text_column] = round(text_row[text_column], 2)
                    worksheet.write(row_num, column_num, text_row[text_column], style[text_column])

            workbook.close()
            return True
        except Exception as e:
            print(str(type(e).__name__), str(e))
            return False

    def join_analytics(self, analytics, all_sku, all_promo):

        self.inf_total_join = {
            'success': False,
            'len_analytics': len(analytics),
            'len_sku': len(all_sku),
            'len_promo': len(all_promo),
            'index_errors': [],
            'history': [],
        }

        main_list = []

        for item in analytics:
            temp = {
                'success': False,
                'text_error': '',
                'id': item,
                'sku': False,
                'promo': False,
            }
            try:
                views = 0
                clicks = 0
                ordered_units = analytics[item].get('ordered')
                revenue = analytics[item].get('revenue')
                SKU_count = 0
                PROMO_count = 0

                if revenue != 0 or ordered_units != 0:
                    avg_sell = revenue / ordered_units
                else:
                    avg_sell = 0

                promo = all_promo.get(item)
                sku = all_sku.get(item)

                if promo:
                    PROMO_count += promo.get('Расход')
                    temp['promo'] = True

                if sku:
                    views += sku.get('Показы')
                    clicks += sku.get('Клики')
                    SKU_count += sku.get('Расход')
                    temp['sku'] = True

                if clicks != 0 or views:
                    CTR = clicks / (views / 100)
                else:
                    CTR = 0

                count = PROMO_count + SKU_count

                if count != 0 and revenue:
                    DRR = (count / revenue) * 100
                else:
                    DRR = 0

                main_dict = {"name": analytics[item]['name'],
                             "id": item,
                             "views": views,
                             "CTR": CTR,
                             "clicks": clicks,
                             "ordered_units": ordered_units,
                             "revenue": revenue,
                             'SKU_count': SKU_count,  # Трафареты
                             'PROMO_count': PROMO_count,  # Продвижение в поиске
                             'avg_sell': avg_sell,
                             "DRR": DRR}

                main_list.append(main_dict)
                temp['success'] = True
            except Exception as e:
                print((str(type(e).__name__), str(e)), (revenue, ordered_units), (clicks, views))
                temp['text_error'] = (str(type(e).__name__), str(e))
                traceback.print_exc()

            self.inf_total_join['history'].append(temp)

        self.inf_total_join['success'] = all([info['success'] for info in self.inf_total_join['history']])
        self.inf_total_join['index_errors'] = [index for index, info in enumerate(self.inf_total_join['history']) if
                                               not info['success']]
        return main_list

    def transform(self, analytics):

        list_ = []

        for info in analytics:
            try:
                uid = info["dimensions"][0]["id"]
                list_.append(uid)
            except Exception as e:
                print("Cannot take uid from analytics", str(type(e).__name__), str(e))
                uid = info
            list_.append(uid)

        self.inf_transform_data.update({
            'success': False,
            'all_uid': list_,
            'history': []
        })

        new_data = {}
        for index, info in enumerate(analytics):

            temp_temp = {'num': index, 'success': False, 'repeats': False, 'error_text': ''}

            try:
                uid = info["dimensions"][0]["id"]
                new_dict = {
                    'id': uid,
                    'name': info["dimensions"][0]["name"],
                    'date': info["dimensions"][1]["id"],
                    'revenue': int(info["metrics"][0]),
                    'ordered': int(info["metrics"][1]),
                }

                if uid not in new_data:
                    new_data[uid] = new_dict
                else:
                    new_data[uid]['revenue'] += new_dict['revenue']
                    new_data[uid]['ordered'] += new_dict['ordered']
                    temp_temp['repeats'] = True

                temp_temp['uid'] = uid
                temp_temp['success'] = True

            except Exception as e:
                error = str(type(e).__name__), str(e)
                temp_temp['error_text'] = error
            self.inf_transform_data['history'].append(temp_temp)

        self.inf_transform_data['success'] = all((info['success'] for info in self.inf_transform_data['history']))
        return new_data
