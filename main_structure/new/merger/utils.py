import requests


def get_analytics_params(date_from: str, date_to: str, seller_id: str, seller_key: str, metrics=None):
    if metrics is None:
        metrics = ['revenue', 'ordered_units']
    return {"url": 'https://api-seller.ozon.ru/v1/analytics/data',
            "headers": {
                "Client-Id": seller_id,
                "Api-Key": seller_key,
                "Content-Type": "application/json"
            },
            "body": {
                "date_from": date_from,
                "date_to": date_to,
                "metrics": metrics,
                "dimension": [
                    "sku",
                    "day"
                ],
                "filters": [],
                "sort": [
                    {
                        "key": "revenue",
                        "order": "DESC"
                    }
                ],
                "limit": 1000,
                "offset": 0
            }}


def get_analytics(date_from, date_to, seller_id, seller_api_key, metrics=None):
    if metrics is None:
        params = get_analytics_params(date_from, date_to, seller_id, seller_api_key)
    else:
        params = get_analytics_params(date_from, date_to, seller_id, seller_api_key, metrics)
    result = requests.post(url=params["url"], headers=params["headers"], json=params["body"])
    try:
        if result.status_code == 200:
            data = result.json()
            return True, data["result"]["data"]
        else:
            return False, result.text
    except Exception as e:
        print((str(type(e).__name__), str(e), result.json()))
        return False, (str(type(e).__name__), str(e), result.json())


def update_metrics(data_dict, metrics, metric_names):
    for metric_name, metric_value in zip(metric_names, metrics):
        if metric_name in data_dict:
            if metric_name == 'position_category':
                data_dict[metric_name] = f' {data_dict[metric_name]}{int(metric_value)} '
                continue
            data_dict[metric_name] += int(metric_value)
        else:
            if metric_name == 'position_category':
                data_dict[metric_name] = f'{int(metric_value)} '
                continue
            data_dict[metric_name] = int(metric_value)
