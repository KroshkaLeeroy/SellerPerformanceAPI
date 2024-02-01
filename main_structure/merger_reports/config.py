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
