import os.path

from flask import Flask, request, jsonify
from flask import send_file
from flask_restx import Api, Resource
from threading import Thread
from application.request_controller import ControllerRequests
from application.utils import add_user_query_to_history, check_if_query_history_exists
from main_structure.new.utils import read_json



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


@api.route('/download-stats/<uid>/<date_from>/<date_to>/<stat_type>/<file_name>', methods=['GET'])
class DownloadStat(Resource):
    def get(self, uid, date_from, date_to, stat_type, file_name):
        try:
            date = f'{date_from}_{date_to}'
            stat_file = read_json(f'downloads/{uid}/{date}/stat/{stat_type}/{file_name}.json')
            return jsonify(stat_file)
        except Exception as e:
            print(e)
            return {'status': 'bad request'}, 400

