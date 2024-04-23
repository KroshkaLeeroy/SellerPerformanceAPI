import os.path

from flask import Flask, request, jsonify
from flask import send_file
from flask_restx import Api, Resource
from threading import Thread
from application.request_controller import ControllerRequests
from application.utils import add_user_query_to_history, check_if_query_history_exists
from main_structure.new.utils import read_json, write_json
from application.config import ADMIN_KEY
import logging


logging.basicConfig(filename='log.txt', filemode='a')

app = Flask(__name__)
api = Api(app)

app.config.from_object('application.config')

controller = ControllerRequests()
thread = Thread(target=controller.main_cycle)
thread.daemon = True
thread.start()


# TODO: Добавить проверку шифрованного ключа на соответствие секрету
@api.route('/check-pull/<uid>', methods=['GET'])
class Pull(Resource):
    def get(self, uid):
        data = check_if_query_history_exists('history.json')
        user_pull = data['users'].get(uid)
        if user_pull:
            return user_pull, 200
        return {'error': 'user not found'}, 404


@api.route('/add-request', methods=['POST'])
class AddRequest(Resource):
    def post(self):
        data = request.json
        if data:
            print(data)
            controller.queue.enqueue(data)
            add_user_query_to_history('history.json', data)
            return {'status': 'ok'}, 200
        return {'status': 'bad request'}, 400


@api.route('/download-report/<uid>/<date_from>/<date_to>', methods=['GET'])
class DownloadReport(Resource):
    def get(self, uid, date_from, date_to):
        try:
            history = read_json('history.json')['users'][uid]['history']
            path = None
            for info in history:
                if info['time_from'] == date_from and info['time_to'] == date_to:
                    path = info['path']
                    break
            if not path:
                return {'status': 'bad request'}, 400
            path = os.path.abspath(path)
            return send_file(path, as_attachment=True)
        except Exception as e:
            print(e)
            return {'status': 'bad request'}, 400


@api.route('/<uid>/<path>', methods=['GET'])
class PathFiles(Resource):
    def get(self, uid, path):
        try:
            if uid == ADMIN_KEY:
                path = path.replace('*', '/').replace('%2F', '/')
                stat_file = os.listdir(os.path.abspath(path))
                stat_file = sorted(stat_file)
                return jsonify(stat_file)
            return {'status': 'bad request'}, 400
        except Exception as e:
            print(e)
            if 'Not a directory' in str(e):
                path = path.replace('*', '/')
                path = os.path.abspath(path)
                return send_file(path, as_attachment=True)
            return {'status': str(e)}, 400



@api.route('/<uid>/delete-status/<user_id>/<status_id>', methods=['GET'])
class DeleteStatus(Resource):
    def get(self, uid, user_id, status_id):
        try:
            if uid == ADMIN_KEY:
                data = check_if_query_history_exists('history.json')
                find = False
                for index in data['users'][user_id]['history']:
                    if index['status_id'] == status_id:
                        data['users'][user_id]['history'].remove(index)
                        find = True
                        break
                if find:
                    write_json(data, 'history.json')
                    return {'status': 'ok'}, 200
                return {'status': 'index not found'}, 400
            return {'status': 'invalid admin key'}, 400
        except Exception as e:
            print(e)
            return {'status': 'error'}, 400

@api.route('/<uid>/<user_id>/<date_from>/<date_to>/<file_path>', methods=['GET'])
class DownloadStat(Resource):
    def get(self, uid, user_id, date_from, date_to,file_path):
        try:
            if uid == ADMIN_KEY:
                date = f'{date_from}_{date_to}'
                path = f'downloads/{user_id}/{date}/stat/{file_path}'
                stat_file = read_json(path)
                return jsonify(stat_file)
            return {'status': 'invalid admin key'}, 400
        except Exception as e:
            print(e)
            return {'status': 'bad request'}, 400