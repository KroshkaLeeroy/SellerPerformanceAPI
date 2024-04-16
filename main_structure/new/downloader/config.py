def get_token_params(id_: str, secret: str):
    return {
        "url": 'https://performance.ozon.ru:443/api/client/token',
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json"
        },
        "params": {
            "client_id": id_,
            "client_secret": secret,
            "grant_type": "client_credentials"
        }
    }


def get_list_of_id_params(token: str) -> dict:
    return {
        "url": 'https://performance.ozon.ru/api/client/campaign',
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
            'Authorization': f'Bearer {token}'
        },
        "params": {
            # "campaignIds": "",
            # "advObjectType": "",
            "state": "CAMPAIGN_STATE_UNKNOWN"
        }
    }


def get_uuid_params(token: str, date_from: str, date_to: str, campaigns_id: list) -> dict:
    return {
        "url": 'https://performance.ozon.ru:443/api/client/statistics',
        "headers": {
            "Content-Type": "application/json",
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        },
        "params": {
            "campaigns": campaigns_id,
            "from": f"{date_from}T00:00:00.000Z",
            "to": f"{date_to}T23:59:59.000Z",
            "dateFrom": date_from,
            "dateTo": date_to,
            "groupBy": "NO_GROUP_BY"
        }
    }


def get_check_uuid_status_params(token: str, uuid: str) -> dict:
    return {
        'url': f'https://performance.ozon.ru/api/client/statistics/{uuid}',
        'headers': {
            "Content-Type": "application/json",
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
    }


def get_download_file_params(token: str, uuid: str) -> dict:
    return {
        'url': f"https://performance.ozon.ru/api/client/statistics/report?UUID={uuid}",
        'headers': {
            'Authorization': f'Bearer {token}'
        }
    }