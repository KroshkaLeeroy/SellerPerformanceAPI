import os.path
import time

from main_structure.new.unpack.unpack_zip import ZipUnpack
from main_structure.new.reader.reader import Reader
from main_structure.new.downloader.downloader import DownloadZip
from main_structure.new.merger.utils import get_analytics, update_metrics
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
        self.inf_total_process = None

        self.inf_transform_data = {}

        self.inf_total_join = {}

    def run(self):
        self.downloader.run()
        self.unpacker.run()
        sku, promo = self.reader.run()
        self.create_report(sku, promo)

    def create_report(self, sku, promo):
        metrics_1 = ['revenue', 'ordered_units',
                     'hits_view_search', 'hits_view_pdp', 'hits_view',
                     'hits_tocart_search', 'hits_tocart_pdp', 'hits_tocart',
                     'session_view_search', 'session_view_pdp', 'session_view',
                     'conv_tocart_search', 'conv_tocart_pdp', 'conv_tocart']

        metrics_2 = ['revenue', 'cancellations', 'returns', 'delivered_units', 'position_category']

        status, info = get_analytics(self.date_from, self.date_to, self.seller_id, self.seller_api_key, metrics_1)
        time.sleep(90)
        status_2, info_2 = get_analytics(self.date_from, self.date_to, self.seller_id, self.seller_api_key, metrics_2)

        self.inf_transform_data = {
            'get_reports_success': status,
            'get_reports_success_2': status_2,
            'join_reports_success': False,
            'create_report_success': False,
        }

        metrics_2[0] = 'useless'

        info = self.transform(info, info_2, metrics_1, metrics_2)
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

            title_list = {
                'Наименование': (400, ["", "", ""]),
                'Ozon ID': (80, ["", "", ""]),
                'Показы': (60, ['C1', '=SUM(C3:C100000)', number_cell_format],),
                'Показы\n(поиск/категория)': (75, ['D1', '=SUM(D3:D100000)', number_cell_format],),
                'Показы\n(карточка)': (75, ['E1', '=SUM(E3:E100000)', number_cell_format],),
                'Показы\n(всего)': (60, ['F1', '=SUM(F3:F100000)', number_cell_format],),
                'В корзину\n(поиск/категория)': (75, ['G1', '=SUM(G3:G100000)', number_cell_format],),
                'В корзину\n(карточка)': (75, ['H1', '=SUM(H3:H100000)', number_cell_format],),
                'В корзину\n(всего)': (70, ['I1', '=SUM(I3:I100000)', number_cell_format],),
                'Уникальные посетители\n(поиск/категория)': (90, ['J1', '=SUM(J3:J100000)', number_cell_format],),
                'Уникальные посетители\n(карточка)': (90, ['K1', '=SUM(K3:K100000)', number_cell_format],),
                'Уникальные посетители\n(всего)': (90, ['L1', '=SUM(L3:L100000)', number_cell_format],),
                'Конверсия\n(поиск/категория)': (75, ['M1', '=AVERAGEIF(M3:M10000, ">0")', summary_cell_format],),
                'Конверсия\n(карточка)': (75, ['N1', '=AVERAGEIF(N3:N10000, ">0")', summary_cell_format],),
                'Конверсия\n(всего)': (75, ['O1', '=AVERAGEIF(O3:O10000, ">0")', summary_cell_format],),
                'CTR': (50, ['P1', '=AVERAGEIF(P3:P10000, ">0")', summary_cell_format],),
                'Клики': (60, ['Q1', '=SUM(Q3:Q100000)', number_cell_format],),
                'Заказано товаров': (70, ['R1', '=SUM(R3:R100000)', number_cell_format],),
                'Конверсия в\nзаказ': (75, ['S1', '=(R1/(I1/100))*0.01', summary_cell_format],),
                'Заказано на сумму': (75, ['T1', '=SUM(T3:T100000)', value_cell_format],),
                'Трафареты': (80, ['U1', '=SUM(U3:U100000)', value_cell_format],),
                'Продвижение в поиске': (100, ['V1', '=SUM(V3:V100000)', value_cell_format],),
                'Ср. цена продажи': (65, ['W1', '=AVERAGEIF(W3:W10000, ">0")', value_cell_format],),
                'Отмены': (65, ['X1', '=SUM(X3:X100000)', number_cell_format],),
                'Возвраты': (65, ['Y1', '=SUM(Y3:Y100000)', number_cell_format],),
                'Отмены и\nвозвраты': (65, ['Z1', '=SUM(Z3:Z100000)', number_cell_format],),
                'Доставлено\nтоваров': (65, ['AA1', '=SUM(AA3:AA100000)', number_cell_format],),
                'Рейтинг\nтовара': (60, ['AB1', '=AVERAGEIF(AB3:AB100000, ">0")', number_cell_format],),
                'ДРР\n%': (60, ['AC1', '=AVERAGEIF(AC3:AC100000, ">0")', DRR_cell_format],),
                'Комментарий': (500, ["", "", ""])
            }

            for value in title_list.values():
                column, formula, format_style = value[1]
                if column == '':
                    continue
                worksheet.write(column, formula, format_style)

            for index, title_column in enumerate(title_list):
                worksheet.write(1, index, title_column, title_cell_format)
                pixels = title_list[title_column][0]
                worksheet.set_column_pixels(index, index, pixels)

            worksheet.set_row_pixels(1, 90)
            worksheet.autofilter('A2:AC100000')
            worksheet.freeze_panes(2, 0)

            worksheet.conditional_format('AC3:AC10000',
                                         {'type': 'cell', 'criteria': '=', 'value': 0,
                                          'format': conditional_1_DRR_cell_format})
            worksheet.conditional_format('AC3:AC10000',
                                         {'type': 'cell', 'criteria': '<=', 'value': 20,
                                          'format': conditional_2_DRR_cell_format})
            worksheet.conditional_format('AC3:AC10000',
                                         {'type': 'cell', 'criteria': '<', 'value': 30,
                                          'format': conditional_3_DRR_cell_format})
            worksheet.conditional_format('AC3:AC10000',
                                         {'type': 'cell', 'criteria': '>=', 'value': 30,
                                          'format': conditional_4_DRR_cell_format})

            for row_num, text_row in enumerate(data_list, 2):
                text_row['comment'] = ''
                for column_num, text_column in enumerate(text_row, ):
                    style = {
                        'name': name_cell_format,
                        'id': number_cell_format,
                        'views_sku': number_cell_format,
                        "hits_view_search": number_cell_format,
                        "hits_view_pdp": number_cell_format,
                        "hits_view": number_cell_format,
                        "hits_tocart_search": number_cell_format,
                        "hits_tocart_pdp": number_cell_format,
                        "hits_tocart": number_cell_format,
                        "session_view_search": number_cell_format,
                        "session_view_pdp": number_cell_format,
                        "session_view": number_cell_format,
                        "conv_tocart_search": number_cell_format,
                        "conv_tocart_pdp": number_cell_format,
                        "conv_tocart": number_cell_format,
                        'CTR': procent_cell_format,
                        'clicks': number_cell_format,
                        'ordered_units': number_cell_format,
                        'conv': procent_cell_format,
                        'revenue': value_cell_format,
                        'SKU_count': value_cell_format,
                        'PROMO_count': value_cell_format,
                        'avg_sell': value_cell_format,
                        'cancellations': number_cell_format,
                        'returns': number_cell_format,
                        'cancellations_returns': number_cell_format,
                        'delivered_units': number_cell_format,
                        'position_category': number_cell_format,
                        'DRR': DRR_column_cell_format,
                        'comment': name_cell_format,
                    }
                    if text_column == 'CTR' or text_column == 'DRR':
                        text_row[text_column] = round(text_row[text_column], 2)
                        if text_column == 'DRR' and text_row[text_column] == 0:
                            text_row[text_column] = '-'
                    elif text_column == "position_category":
                        try:
                            text_row[text_column] = int(text_row[text_column])
                        except Exception as e:
                            print(str(type(e).__name__), str(e), traceback.format_exc())
                    worksheet.write(row_num, column_num, text_row[text_column], style[text_column])

            workbook.close()
            return True
        except Exception as e:
            print(str(type(e).__name__), str(e), traceback.format_exc())
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
            views = 0
            clicks = 0
            ordered_units = analytics[item].get('ordered_units')
            revenue = analytics[item].get('revenue')
            SKU_count = 0
            PROMO_count = 0
            try:
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
                if ordered_units != 0 and analytics[item].get('hits_tocart') != 0:
                    conv = (ordered_units / (analytics[item].get('hits_tocart') / 100)) * 0.01
                else:
                    conv = 0

                main_dict = {"name": analytics[item]['name'],
                             "id": item,
                             "views_sku": views,
                             "hits_view_search": analytics[item].get('hits_view_search'),
                             "hits_view_pdp": analytics[item].get('hits_view_pdp'),
                             "hits_view": analytics[item].get('hits_view'),
                             "hits_tocart_search": analytics[item].get('hits_tocart_search'),
                             "hits_tocart_pdp": analytics[item].get('hits_tocart_pdp'),
                             "hits_tocart": analytics[item].get('hits_tocart'),
                             "session_view_search": analytics[item].get('session_view_search'),
                             "session_view_pdp": analytics[item].get('session_view_pdp'),
                             "session_view": analytics[item].get('session_view'),
                             "conv_tocart_search": analytics[item].get('conv_tocart_search'),
                             "conv_tocart_pdp": analytics[item].get('conv_tocart_pdp'),
                             "conv_tocart": analytics[item].get('conv_tocart'),
                             "CTR": CTR,
                             "clicks": clicks,
                             "ordered_units": ordered_units,
                             'conv': conv,
                             "revenue": revenue,
                             'SKU_count': SKU_count,  # Трафареты
                             'PROMO_count': PROMO_count,  # Продвижение в поиске
                             'avg_sell': avg_sell,
                             'cancellations': analytics[item].get('cancellations'),
                             'returns': analytics[item].get('returns'),
                             'cancellations_returns': analytics[item].get('cancellations') +
                                                      analytics[item].get('returns'),
                             'delivered_units': analytics[item].get('delivered_units'),
                             'position_category': analytics[item].get('position_category'),
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

    def process_analytics(self, analytics_data, new_data, metric_names):
        for index, info in enumerate(analytics_data):
            temp_temp = {'num': index, 'success': False, 'repeats': False, 'error_text': ''}
            try:
                uid = info["dimensions"][0]["id"]
                if uid not in new_data:
                    new_data[uid] = {
                        'id': uid,
                        'name': info["dimensions"][0]["name"],
                        'date': info["dimensions"][1]["id"]
                    }

                update_metrics(new_data[uid], info["metrics"], metric_names)
                temp_temp['uid'] = uid
                temp_temp['success'] = True
            except Exception as e:
                error = str(type(e).__name__), str(e)
                temp_temp['error_text'] = error
                self.inf_total_process['history'].append(temp_temp)

    def transform(self, analytics, analytics2, metric_names_set1, metric_names_set2):

        list_ = []

        for info in analytics:
            try:
                uid = info["dimensions"][0]["id"]
                # list_.append(uid)
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

        self.process_analytics(analytics, new_data, metric_names_set1)
        self.process_analytics(analytics2, new_data, metric_names_set2)

        self.inf_transform_data['success'] = all((info['success'] for info in self.inf_transform_data['history']))
        return new_data


if __name__ == '__main__':
    merger = Merger('2024-06-06',
                    '2024-06-06',
                    )
    merger.run()
