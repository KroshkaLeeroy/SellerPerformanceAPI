import datetime
import time


class RequestsController:
    def __init__(self):
        self.requests_pull = []

    def main_cycle(self):
        while True:
            self.load_from_db()
            for request in self.requests_pull:
                status = request.request_status
                if status == 'need_to_process':
                    print(f'!!! Отчет №{request.id} от {request.login} в процессе скачивания')
                    # main_merger = MergerReports(
                    #     request.date_from,
                    #     request.date_to,
                    #     request.client_id_performance,
                    #     request.api_key_performance,
                    #     request.client_id_seller,
                    #     request.api_key_seller,
                    #     request.login,
                    #     debug=True
                    # )
                    # main_merger.start_download()
                    # merger_status = main_merger.merge_reports()
                    time.sleep(5)
                    request.request_status = 'ready_to_download'
                    request.path_to = request.login + '/маршрут нахуй'

                    db.session.commit()
                elif status == 'ready_to_download':
                    print(f'Отчет №{request.id} от {request.login} готов к скачиванию')
                elif status == 'some_error':
                    print(f'Отчет №{request.id} от {request.login} ошибки обработки отчетов')
            time.sleep(20)

    def add_request(self, data):
        request = Request(
            login=data['login'],
            api_key_seller=data['api_key_seller'],
            client_id_seller=data['client_id_seller'],
            api_key_performance=data['api_key_performance'],
            client_id_performance=data['client_id_performance'],
            date_from=data['date_from'],
            date_to=data['date_to'],
            time_start=datetime.datetime.now(),
        )
        db.session.add(request)
        db.session.commit()

    def load_from_db(self):
        with app.app_context():
            self.requests_pull = Request.query.filter_by(request_status='need_to_process').all()
