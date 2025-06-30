import requests
import gzip
import io
import json
import os


class Financeiro:
    def __init__(self, access_token):
        self.url_financeiro_reconciliation = 'https://merchant-api.ifood.com.br/financial/v3.0/merchants/{merchantId}/reconciliation'
        self.url_financeiro_events = 'https://merchant-api.ifood.com.br/financial/v3.0/merchants/{merchantId}/financial-events'
        self.access_token = access_token
        self.list_merchant_path = 'merchant_list.json'
        self.competencia = '2025-06'

    # Consume Endpoint reconciliation
    def consume_dados(self):
        if not os.path.exists(self.list_merchant_path):
            print(f"Arquivo {self.list_merchant_path} não encontrado.")
            return

        with open(self.list_merchant_path, 'r', encoding='utf-8') as f:
            merchants = json.load(f).get("list_merchantid", [])

        for empresa in merchants:
            razao = empresa.get("razao")
            merchant_id = empresa.get("merchantId")
            url = self.url_financeiro_reconciliation.format(merchantId=merchant_id)
            params = {
                "competence": self.competencia,
            }

            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json"
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()

                print(f'Empresa: {razao}')
                print("downloadPath:", data.get('downloadPath'))

                # DOWNLOAD AUTOMÁTICO PLANILHA
                # if "downloadPath" in data:
                #     download_url = data["downloadPath"]
                #     file_response = requests.get(download_url)
                #     if file_response.status_code == 200:
                #         with gzip.GzipFile(fileobj=io.BytesIO(file_response.content)) as gz:
                #             csv_content = gz.read()
                #             downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
                #             os.makedirs(downloads_path, exist_ok=True)
                #             file_name = f"reconciliation_{razao}_{self.competencia.replace('-', '_')}.csv"
                #             full_path = os.path.join(downloads_path, file_name)
                #             with open(full_path, 'wb') as f:
                #                 f.write(csv_content)
                #             print('Arquivo Salvo em: ', full_path)
                #             continue
                #     else:
                #         print(f"Erro: {file_response.status_code}")
                # else:
                #     print("Não encontrado downloadPath para Download do arquivo.")
            else:
                print(f"Erro: {response.status_code}: {response.text}")
                return None

    # Consume Endpoint financial_events
    def consume_financerio_eventos(self):
        if not os.path.exists(self.list_merchant_path):
            print(f"Arquivo {self.list_merchant_path} não encontrado.")
            return

        with open(self.list_merchant_path, 'r', encoding='utf-8') as f:
            merchants = json.load(f).get("list_merchantid", [])

        for empresa in merchants:
            merchant_id = empresa.get("merchantId")
            url = self.url_financeiro_events.format(merchantId=merchant_id)
            params = {
                "beginDate": "2025-06-01",
                "endDate": "2025-06-25",
                "page": "1",
                "size": "100"
            }

            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json"
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                print(data)
                return
            else:
                print(f"Erro: {response.status_code}: {response.text}")
                return None
