import datetime
import json
import os.path
import time

from flask import Flask, request
from flask import send_file
from flask_restx import Api, Resource
from threading import Thread
from main_structure import MergerReports, logging
from application.config import DEBUG, URL_TO_API

app = Flask(__name__)
api = Api(app)

app.config.from_object('application.config')


def read():
    with open('pull_data.json', 'r', encoding="utf-8") as f:
        data = json.loads(f.read())
        return data


# sample = {
#     "last_id": 0,
#     'list': [
#         {
#             'id': 1,
#             'status': "need_to_process",
#             'email': 'sample',
#             'api_key_seller': 'sample',
#             'client_id_seller': 'sample',
#             'api_key_performance': 'sample',
#             'client_id_performance': 'sample',
#             'date_from': 'sample',
#             'date_to': 'sample',
#         },
#     ]
# }

def write(data):
    with open('pull_data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f)


class RequestsController:
    def __init__(self):
        self.requests_pull = []

    def main_cycle(self):
        while True:
            old_data = self.load_from_db()
            self.requests_pull = self.load_from_db(True)
            for index, request__ in enumerate(self.requests_pull['list']):
                status = request__.get('status')
                if status == 'need_to_process':
                    merger = MergerReports(request__.get('date_from'),
                                           request__.get('date_to'),
                                           request__.get('client_id_performance'),
                                           request__.get('api_key_performance'),
                                           request__.get('client_id_seller'),
                                           request__.get('api_key_seller'),
                                           request__.get('email'),
                                           debug=DEBUG)
                    try:
                        merger.start_download()
                        ready = merger.merge_reports()
                        # TODO одновременное скачивание PROMO SKU, так быть не должно, на выходе почему-то 2 отчета

                        if not ready:
                            request__['status'] = "ready_to_download"
                            request__['path'] = merger.total_exel_path
                        else:
                            request__['status'] = "some_error"
                    except Exception as e:
                        logging.warning(
                            f'При формировании или скачивании отчета, что-то пошло не так! Проверяй логи!! {e}')
                        request__['status'] = "some_error"

                    old_data['list'][request__.get('id') - 1] = request__
                    er_mes = f'Отчет №{request__.get("id")} от {request__.get("email")} был скачан'
                    logging.info(er_mes)
                    print(er_mes)

                elif status == 'ready_to_download':
                    er_mes = f'Отчет №{request__.get("id")} от {request__.get("email")} готов к скачиванию'
                    logging.info(er_mes)
                    print(er_mes)
                elif status == 'some_error':
                    er_mes = f'Отчет №{request__.get("id")} от {request__.get("email")} ошибки обработки отчетов'
                    logging.info(er_mes)
                    print(er_mes)

            write(old_data)
            time.sleep(20)

    def add_request(self, data):
        current_datetime = datetime.datetime.now()
        formatted_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        current_data = read()
        last_id = current_data.get('last_id') + 1
        current_data['last_id'] = last_id
        current_data['list'].append(
            {
                'id': last_id,
                'status': "need_to_process",
                'email': data.get('email'),
                'api_key_seller': data.get('api_key_seller'),
                'client_id_seller': data.get('client_id_seller'),
                'api_key_performance': data.get('api_key_performance'),
                'client_id_performance': data.get('client_id_performance'),
                'date_create': formatted_string,
                'date_from': data.get('date_from'),
                'date_to': data.get('date_to'),
                'path': None,
            }
        )
        write(current_data)

    def add_fake_request(self, mail):
        current_datetime = datetime.datetime.now()
        formatted_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

        current_data = read()
        last_id = current_data.get('last_id') + 1
        current_data['last_id'] = last_id
        current_data['list'].append(
            {
                'id': last_id,
                'status': "ready_to_download",
                'email': mail,
                'api_key_seller': 'some_fake_api_key_seller',
                'client_id_seller': 'some_fake_client_id_seller',
                'api_key_performance': 'some_fake_api_key_performance',
                'client_id_performance': 'some_fake_client_id_performance',
                'date_create': formatted_string,
                'date_from': 'some_fake_date_from',
                'date_to': 'some_fake_date_to',
                'path': f'reports/fake_report.xlsx',
            }
        )
        write(current_data)

    def load_from_db(self, filter=False):
        data = read()
        if filter:
            data = self.filter('status', "need_to_process", data)
        return data

    def filter(self, key, param, data):
        new_data = {
            'last_id': None,
            'list': []
        }
        for item in data['list']:
            if item.get(key) == param:
                new_data['list'].append(item)

        new_data['last_id'] = data.get('last_id')
        return new_data

    def get_requests_by_email(self, email):
        data = self.load_from_db()
        data = self.filter("email", email, data)
        return data['list']

    def get_request_by_id(self, mail, uid):
        data = self.load_from_db()
        report = data['list'][uid - 1]
        if mail == report['email']:
            if report['status'] == 'ready_to_download':
                return report['path']
        return None


def if_pull_json_exist():
    if not os.path.isfile('pull_data.json'):
        write({'last_id': 0,
               'list': []})


if_pull_json_exist()
controller = RequestsController()
thread = Thread(target=controller.main_cycle)
thread.daemon = True
thread.start()

# TODO добавить шифрование почты, ключей API


@api.route('/check-pull/<email>', methods=['GET'])
class CheckPull(Resource):
    def get(self, email):
        try:
            data = controller.get_requests_by_email(email)
            return data, 200
        except Exception as e:
            er_mes = f'При проверке пула запроса {email}, что то пошло не так, {e}'
            logging.error(er_mes)
            return {'message': er_mes}


@api.route('/make-fake/<mail>')
class FakeRequest(Resource):
    def get(self, mail):
        try:
            controller.add_fake_request(mail)
            return {'message': 'fake_request_created'}, 200
        except Exception as e:
            er_mes = f'При добавлении фейкового запроса, что то пошло не так, {e}'
            logging.error(er_mes)
            return {'message': er_mes}


@api.route('/add-request', methods=['POST'])
class AddRequestToPull(Resource):
    def post(self):
        try:
            # Получение данных из тела JSON-запроса
            data = request.get_json()

            data = {
                'email': data.get('email'),
                'api_key_seller': data.get('api_key_seller'),
                'client_id_seller': data.get('client_id_seller'),
                'api_key_performance': data.get('api_key_performance'),
                'client_id_performance': data.get('client_id_performance'),
                'date_from': data.get('date_from'),
                'date_to': data.get('date_to'),
            }

            controller.add_request(data)

            return {'message': 'request_accepted'}, 200

        except Exception as e:
            return {'message': str(e)}, 500


@api.route('/download-report/<email>/<int:uid>', methods=['GET'])
class DownloadReport(Resource):
    def get(self, email, uid):
        try:
            relative_path = controller.get_request_by_id(email, uid)
            logging.info(relative_path)

            if relative_path:
                # Получение абсолютного пути
                absolute_path = os.path.abspath(relative_path)

                # Проверка существования файла
                if os.path.exists(absolute_path):
                    return send_file(absolute_path, as_attachment=True)
                else:
                    return {'message': f'File not found at path: {absolute_path}'}, 404
            else:
                return {'message': 'Report not found or not ready for download.'}, 404

        except Exception as e:
            er_mes = f'Error during report download: {str(e)}'
            logging.error(er_mes)
            return {'message': er_mes}, 500
