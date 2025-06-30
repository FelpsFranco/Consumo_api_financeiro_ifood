import requests
import credenciais
from finaceiro_consumo import Financeiro
import json
import os


class IfoodAuth:
    def __init__(self):
        self.client_id = credenciais.client_id
        self.client_secret = credenciais.client_secret
        self.arquivo_token = 'refresh.json'
        self.user_code = None
        self.authorization_code_verifier = None
        self.authorization_code = None
        self.access_token = None
        self.refresh_token = None
        self.token_url_user_code = 'https://merchant-api.ifood.com.br/authentication/v1.0/oauth/userCode'
        self.token_url_acess = 'https://merchant-api.ifood.com.br/authentication/v1.0/oauth/token'

    def gera_user_code(self):
        payload = {
            'clientId': self.client_id,
            'clientSecret': self.client_secret
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        resp = requests.post(self.token_url_user_code, data=payload, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            self.user_code = data.get('userCode')
            self.authorization_code_verifier = data.get('authorizationCodeVerifier')

            print("User Code:", data.get("userCode"))
            print("Authorization Code:", data.get("authorizationCodeVerifier"))
            print("Verification URL:", data.get("verificationUrl"))
            print("Verification URL Complete:", data.get("verificationUrlComplete"))
            print("Expires in:", data.get("expiresIn"))
            self.authorization_code = input('\n\nauthorization_code: ')
            self.gera_acess_token()
        else:
            print(f"Erro {resp.status_code}: {resp.text}")

    def gera_acess_token(self, ):
        payload = {
            'grantType': 'authorization_code',
            'clientId': self.client_id,
            'clientSecret': self.client_secret,
            'authorizationCode': self.authorization_code,
            'authorizationCodeVerifier': self.authorization_code_verifier
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        resp = requests.post(self.token_url_acess, data=payload, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            self.access_token = data.get('accessToken')
            self.refresh_token = data.get('refreshToken')
            print('Access Token:', data.get('accessToken'))
            print('Refresh Token:', data.get('refreshToken'))
            print('Expires In:', data.get('expiresIn'))
            self.armazena_refresh_token()
        else:
            print(f"Erro {resp.status_code}: {resp.text}")

    def armazena_refresh_token(self):
        data = {
            "accessToken": self.access_token,
            "refreshToken": self.refresh_token
        }
        with open(self.arquivo_token, 'w') as f:
            json.dump(data, f)

    def carregar_token(self):
        if os.path.exists(self.arquivo_token):
            with open(self.arquivo_token, 'r') as f:
                content = f.read()
                if content.strip():
                    return json.loads(content)
        return None

    def renovar_token_com_refresh(self, refresh_token):
        payload = {
            'grantType': 'refresh_token',
            'clientId': self.client_id,
            'clientSecret': self.client_secret,
            'refreshToken': refresh_token
        }

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        resp = requests.post(self.token_url_acess, data=payload, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            self.access_token = data.get('accessToken')
            self.refresh_token = data.get('refreshToken')
            self.armazena_refresh_token()
            return self.access_token
        else:
            print(f"Erro: {resp.status_code} - {resp.text}")
            return None

    def verifica_existe_refr(self):
        self.refresh_token = self.carregar_token()
        if self.refresh_token:
            self.access_token = self.renovar_token_com_refresh(self.refresh_token.get("refreshToken"))
            if self.access_token:
                finan = Financeiro(self.access_token)
                finan.consume_dados()
                # finan.consume_financerio_eventos()
        else:
            self.gera_user_code()
            finan = Financeiro(self.access_token)
            finan.consume_dados()
            # finan.consume_financerio_eventos()
        return None


if __name__ == "__main__":
    auth = IfoodAuth()
    auth.verifica_existe_refr()
