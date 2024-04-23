from main_structure.new.downloader.utils import *
import time


class DownloadZip:
    def __init__(self, date_from, date_to, client_id, client_secret, client_folder):
        self.client_id = client_id
        self.client_secret = client_secret
        self.files_path_client = os.path.join('downloads', client_folder)
        self.files_path_report = os.path.join('downloads', client_folder, f'{date_from}_{date_to}')
        self.files_path_report_stat = os.path.join('downloads', client_folder, f'{date_from}_{date_to}', 'stat',
                                                   'download')
        self.date_from = date_from
        self.date_to = date_to
        self.date = f'{date_from}_{date_to}'

        self.UUID_GET_START_TIMER = None
        self.DOWNLOADER_LOOP_TIMER = None
        self.UUID_STATUS_TIMER = None
        self.ATTEMPT_TO_GET_UUID = None
        self.inf_initial_data = None
        self.inf_getting_token = None
        self.inf_getting_list_of_IDs = None
        self.inf_getting_split_list = None
        self.inf_downloads_reports = None
        self.inf_total_downloads_reports = None
        self.inf_total_results = None

        self.set_up()

        write_json(self.inf_initial_data, os.path.join(self.files_path_report_stat, '1_initial.json'))

    def set_up(self):
        self.UUID_GET_START_TIMER = 5
        self.DOWNLOADER_LOOP_TIMER = 5
        self.UUID_STATUS_TIMER = 10
        self.ATTEMPT_TO_GET_UUID = 30

        self.inf_initial_data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'files_path_client': self.files_path_client,
            'files_path_report': self.files_path_report,
            'files_path_report_stat': self.files_path_report_stat,
            'created_new_folder_client': ensure_directory_exists(self.files_path_client),
            'create_new_folder_report': ensure_directory_exists(self.files_path_report),
            'create_new_folder_report_stat': ensure_directory_exists(self.files_path_report_stat),
            'date': self.date,
            'date_from': self.date_from,
            'date_to': self.date_to,
            'UUID_GET_START_TIME': self.UUID_GET_START_TIMER,
            'DOWNLOADER_LOOP_TIME': self.DOWNLOADER_LOOP_TIMER,
            'UUID_STATUS_TIMER': self.UUID_STATUS_TIMER,
            'ATTEMPT_TO_GET_UUID': self.ATTEMPT_TO_GET_UUID,
        }

        self.inf_getting_token = {
            'succeeded': False,
            'error_text': '',
            'token': '',
        }
        self.inf_getting_list_of_IDs = {
            'succeeded': False,
            'error_text': '',
            'IDs_list': {'SKU': [], 'PROMO': []},
        }
        self.inf_getting_split_list = {
            'SKU': {
                'succeeded': False,
                'error_text': '',
                'IDs_list': [],
            },
            'PROMO': {
                'succeeded': False,
                'error_text': '',
                'IDs_list': [],
            }
        }
        self.inf_downloads_reports = {
            'SKU': {
                'succeeded': False,
                'succeeded_epoch': {},
                'counter': 0,
                'get_initial_uuid_history': [],
                'lost_companies': [],
                'check_UUID_status_history': [],
                'downloaded_reports_history': []
            },
            'PROMO': {
                'succeeded': False,
                'succeeded_epoch': {},
                'counter': 0,
                'get_initial_uuid_history': [],
                'lost_companies': [],
                'check_UUID_status_history': [],
                'downloaded_reports_history': []
            },
        }
        self.inf_total_downloads_reports = {
            'status': False,
            'request_counter': 0,
            'total_company_list': {
                'SKU': [],
                'PROMO': []
            },
            'skipped_company_list': {
                'SKU': [],
                'PROMO': []
            },
            'time': {
                'SKU': '',
                'PROMO': ''
            }
        }

        self.inf_total_results = {
            'inf_initial_data': {
                'status': False,
                'data': self.inf_initial_data,
            },
            'inf_getting_token': {
                'status': False,
                'data': self.inf_getting_token,
            },
            'inf_getting_list_of_IDs': {
                'status': False,
                'data': self.inf_getting_list_of_IDs,
            },
            'inf_getting_split_list': {
                'status': False,
                'data': self.inf_getting_split_list,
            },
            'inf_downloads_reports': {
                'status': False,
                'data': self.inf_downloads_reports,
            },
            'inf_total_downloads_reports': {
                'status': False,
                'data': self.inf_total_downloads_reports,
            },
            'total_requests': 0,
        }

    def download_files(self):

        result, time_info = get_token(self.client_id, self.client_secret)
        status, value = result

        self.inf_getting_token.update(time_info)
        self.inf_getting_token['succeeded'] = status

        if not status:
            self.inf_getting_token['error_text'] = value
            return False

        self.inf_getting_token['token'] = value
        token = value

        write_json(self.inf_getting_token, os.path.join(self.files_path_report_stat, '2_token.json'))

        result, time_info = get_list_of_ids(token)

        status, value_1, value_2 = result
        self.inf_getting_list_of_IDs.update(time_info)
        self.inf_getting_list_of_IDs['succeeded'] = status
        if not status:
            self.inf_getting_list_of_IDs['error_text'] = f"{value_1}, {value_2}"
            return False

        self.inf_getting_list_of_IDs['IDs_list'] = {'SKU': value_1, 'PROMO': value_2}

        write_json(self.inf_getting_list_of_IDs, os.path.join(self.files_path_report_stat, '3_listIDs.json'))

        for key in ['SKU', 'PROMO']:
            result, time_info = split_list(value_1 if key == 'SKU' else value_2, 10)

            status, value = result
            self.inf_getting_split_list[key].update(time_info)
            self.inf_getting_split_list[key]['succeeded'] = status

            if not status:
                self.inf_getting_split_list[key]['error_text'] = value
                return False

            self.inf_getting_split_list[key]['IDs_list'] = value

            write_json(self.inf_getting_split_list, os.path.join(self.files_path_report_stat, f'4_split{key}.json'))

            result, time_info = self.download_loop(value, token, key)
            self.inf_total_downloads_reports['time'][key] = time_info
            self.inf_downloads_reports[key]['succeeded_epoch'] = result

            write_json(self.inf_downloads_reports, os.path.join(self.files_path_report_stat, f'5_download{key}.json'))

        temp = all((self.inf_downloads_reports['SKU']['succeeded'],
                    self.inf_downloads_reports['PROMO']['succeeded']))
        self.inf_total_downloads_reports['status'] = temp

        write_json(self.inf_total_downloads_reports, os.path.join(self.files_path_report_stat, '6_total_download.json'))

    def run(self):
        self.download_files()

        bool_sum_1 = all((self.inf_getting_split_list['SKU']['succeeded'],
                          self.inf_getting_split_list['PROMO']['succeeded']))

        bool_sum_2 = all((self.inf_downloads_reports['SKU']['succeeded'],
                          self.inf_downloads_reports['PROMO']['succeeded']))

        self.inf_total_results = {
            'inf_initial_data': {
                'status': True,
                'data': self.inf_initial_data,
            },
            'inf_getting_token': {
                'status': self.inf_getting_token['succeeded'],
                'data': self.inf_getting_token,
            },
            'inf_getting_list_of_IDs': {
                'status': self.inf_getting_list_of_IDs['succeeded'],
                'data': self.inf_getting_list_of_IDs,
            },
            'inf_getting_split_list': {
                'status': bool_sum_1,
                'data': self.inf_getting_split_list,
            },
            'inf_downloads_reports': {
                'status': bool_sum_2,
                'data': self.inf_downloads_reports,
            },
            'inf_total_downloads_reports': {
                'status': self.inf_total_downloads_reports['status'],
                'data': self.inf_total_downloads_reports,
            },
            'total_requests': self.inf_total_downloads_reports['request_counter'],
        }

        self.inf_total_results['success'] = all((
            self.inf_total_results['inf_initial_data']['status'],
            self.inf_total_results['inf_getting_token']['status'],
            self.inf_total_results['inf_getting_list_of_IDs']['status'],
            self.inf_total_results['inf_getting_split_list']['status'],
            self.inf_total_results['inf_downloads_reports']['status'],
            self.inf_total_results['inf_total_downloads_reports']['status'],
        ))

        write_json(self.inf_total_results, os.path.join(self.files_path_report_stat, 'main.json'))

    @time_decorator
    def download_loop(self, ides, token, type_):
        succeeded = {
            'get_uuid': True,
            'get_uuid_status': True,
            'download_file': True,
        }
        for counter, list_ in enumerate(ides):
            file_type = "zip" if len(list_) > 1 else "csv"
            result, time_info = self.get_uuid(token, list_, type_)
            status, value = result
            value.update({'time': time_info, 'ID_counter': counter})
            self.inf_downloads_reports[type_]['get_initial_uuid_history'].append(value)
            if not status:
                self.inf_downloads_reports[type_]['lost_companies'].append({
                    'company_IDs': list_,
                    'reason': value['initial_status_error_text']})
                succeeded['get_uuid'] = False
                # TODO добавить проверку повторяющейся ошибки, если одна и та же ошибка повторятся несколько раз
                # останавливать работу приложения, чтобы не тратить количество запросов
                continue

            UUID = value['UUID']

            time.sleep(self.DOWNLOADER_LOOP_TIMER)

            result, time_info = self.get_uuid_status(UUID, token)
            status, value = result
            value.update({'time': time_info, 'ID_counter': counter})
            self.inf_downloads_reports[type_]['check_UUID_status_history'].append(value)
            if not value['status']:
                self.inf_downloads_reports[type_]['lost_companies'].append({
                    'company_IDs': list_,
                    'reason': value['history'][-1]['status_text']})
                succeeded['get_uuid_status'] = False
                continue

            file_path = os.path.join(self.files_path_report, f"{counter}-{type_}.{file_type}")

            result, time_info = download_file_count(token, UUID, file_path)
            status, value = result
            value = {
                'status': status,
                'count_attempts': 1,
                'file_path': file_path,
                'ID_counter': counter,
                'time': time_info,
            }
            self.inf_downloads_reports[type_]['downloaded_reports_history'].append(value)
            if not status:
                self.inf_downloads_reports[type_]['lost_companies'].append({
                    'company_IDs': list_,
                    'reason': value})
                succeeded['download_file'] = False

        self.inf_downloads_reports[type_]['succeeded'] = True if 3 == sum(succeeded.values()) else False

        initial_counter = self.inf_downloads_reports[type_]['get_initial_uuid_history']
        check_counter = self.inf_downloads_reports[type_]['check_UUID_status_history']
        download_counter = self.inf_downloads_reports[type_]['downloaded_reports_history']

        initial_counter = sum([count['count_attempts'] for count in initial_counter])
        check_counter = sum([count['count_attempts'] for count in check_counter])
        download_counter = sum([count['count_attempts'] for count in download_counter])

        summa_counter = initial_counter + check_counter + download_counter

        self.inf_total_downloads_reports['request_counter'] += summa_counter
        self.inf_total_downloads_reports['total_company_list'][type_] = ides

        skipped_companies = self.inf_downloads_reports[type_]['lost_companies']
        self.inf_total_downloads_reports['skipped_company_list'][type_] += skipped_companies

        return succeeded

    @time_decorator
    def get_uuid(self, token, list_of_id, type_):
        start_dict = {
            'type': type_,
            'company_IDs': list_of_id,
            'initial_status': False,
            'initial_status_error_text': '',
            'count_attempts': 0,
            'initial_status_history': [],
            'UUID': '',
        }
        time.sleep(self.UUID_GET_START_TIMER)
        for attempt in range(5):
            result, time_info = get_uuid_count(token, self.date_from, self.date_to, list_of_id)
            status, value = result
            temp = {
                'status': status,
                'error_text': '',
                'attempt_num': attempt,
                'time': time_info,
                'UUID': ''
            }
            if not status:
                temp['error_text'] = value
                start_dict['initial_status_history'].append(temp)
                time.sleep(10)
                continue
            temp['UUID'] = value
            start_dict['initial_status_history'].append(temp)
            start_dict.update({
                'initial_status': status,
                'count_attempts': attempt + 1,
                'UUID': value
            })
            return True, start_dict
        start_dict.update({
            'count_attempts': attempt,
            'initial_status_error_text': 'The number of attempts to get initial UUID has expired'
        })
        return False, start_dict

    @time_decorator
    def get_uuid_status(self, uuid, token):
        attempt_history = {
            'status': False,
            'count_attempts': 0,
            'history': []
        }
        attempt = self.ATTEMPT_TO_GET_UUID
        result, time_info = check_uuid_status_count(uuid, token)
        temp = {
            'status_text': result,
            'attempt': self.ATTEMPT_TO_GET_UUID - attempt,
            'time': time_info
        }
        attempt -= 1
        attempt_history['history'].append(temp)

        while result == "IN_PROGRESS" and attempt > 0:
            time.sleep(self.UUID_STATUS_TIMER)
            result, time_info = check_uuid_status_count(uuid, token)
            temp = {
                'status_text': result,
                'attempt': self.ATTEMPT_TO_GET_UUID - attempt,
                'time': time_info
            }
            attempt -= 1
            attempt_history['history'].append(temp)
        attempt_history['count_attempts'] = self.ATTEMPT_TO_GET_UUID - attempt
        if result == "OK":
            attempt_history['status'] = True
            return True, attempt_history
        return False, attempt_history
