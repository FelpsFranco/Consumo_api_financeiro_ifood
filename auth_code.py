import requests
import json

class Autorizador:
    def __init__(self):
        self.client_id = '88292c6a-e632-4dbf-921a-43d673b08449'
        self.client_secret = 'sy48vhmaqjcwd1v6st50t6tl2tfq167u5vqys9291ajhj0fy5zahqp6iw10cnypck0max9a9ukudv2skkljdvh3uq7felp5345n'
        self.auth_url = 'https://merchant-api.ifood.com.br/authentication/v1.0/oauth/token'

    def consulta_auth(self):
        payload = {
            'grantType': 'client_credentials',
            'clientId': self.client_id,
            'clientSecret': self.client_secret
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(self.auth_url, headers=headers, data=payload)

        if response.status_code == 200:
            token = response.json().get('accessToken')
            if token:
                acess_financeiro = Financeiro(token)
                acess_financeiro.consulta_dados()
            else:
                print("Token não disponível")
        else:
            print("Erro de autenticação:", response.status_code, response.text)


class Financeiro:
    def __init__(self, auth_token):
        self.url = 'https://merchant-api.ifood.com.br/financial/v1.0/events'
        self.auth_token = auth_token

    def consulta_dados(self):
        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Accept': 'application/json'
        }

        params = {
            'beginDate': '2024-06-01T00:00:00Z',
            'endDate': '2024-06-25T23:59:59Z',
            'page': 0,
            'size': 50
        }

        response = requests.get(self.url, headers=headers, params=params)

        if response.status_code == 200:
            dados = response.json()
            print(json.dumps(dados, indent=2))
        else:
            print("Erro ao obter resposta:", response.status_code, response.text)



if __name__ == "__main__":
    autorizador = Autorizador()
    autorizador.consulta_auth()
