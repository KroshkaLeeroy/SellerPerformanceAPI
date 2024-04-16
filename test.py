from main_structure.new.utils import read_json
from application.utils import add_user_query_to_history


def convert_from_old_format_to_new():
    old_data = read_json('pull_data.json')['list']

    counter = 0

    for data in old_data:
        query = {
            'user_id': data['client_id_seller'],
            'date_from': data['date_from'],
            'date_to': data['date_to'],
            'date_create': data['date_create'],
        }
        if data['status'] == 'ready_to_download':
            add_user_query_to_history('history.json', query, data['path'], 'ready', True)
            print(query)
            counter += 1
    print(counter)



convert_from_old_format_to_new()
