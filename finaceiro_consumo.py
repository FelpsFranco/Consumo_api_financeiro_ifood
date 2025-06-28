import requests
import json

class Financeiro:
    def __init__(self, access_token):
        self.url_financeiro_events = 'https://merchant-api.ifood.com.br/financial/v3.0/merchants/{merchantId}/financial-events'
        self.merchant_id = '2807cba8-9bf4-4e71-8e20-be132fa55468'
        self.access_token = access_token
        self.url = self.url_financeiro_events.format(merchantId=self.merchant_id)

    def consume_dados(self):
        params = {
            "beginDate": '2025-06-01',
            "endDate": '2025-06-02',
            "page": '1',
            "size": '5'
        }

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }

        response = requests.get(self.url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))
            return data
        else:
            print(f"Erro: {response.status_code}: {response.text}")
            return None