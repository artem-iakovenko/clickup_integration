import time
import requests
import datetime
from datetime import timedelta


class ZohoAuth:
    def __init__(self, client_id, client_secret, refresh_token):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        self.expiration_date = None

    def get_or_refresh_access_token(self):
        if self.check_expiration():
            access_token_response = requests.post(f'https://accounts.zoho.com/oauth/v2/token?refresh_token={self.refresh_token}&client_id={self.client_id}&client_secret={self.client_secret}&grant_type=refresh_token')
            if access_token_response.status_code == 200:
                response_json = access_token_response.json()
                # print(response_json)
                self.access_token = response_json['access_token']
                current_time = datetime.datetime.now()
                expiry_date = current_time + timedelta(minutes=55)
                self.expiration_date = expiry_date
            else:
                print('Error While Refreshing a Token')
        # self.access_token = '1000.bd8c03ab3f2bf9a0b1de6e719bb37d8f.07784678adcb37dc28187509e4554c09'
        # current_time = datetime.datetime.now()
        # expiry_date = current_time + timedelta(minutes=55)
        # self.expiration_date = expiry_date

    def check_expiration(self):
        expiration_date = self.expiration_date
        access_token = self.access_token
        if not expiration_date or not access_token:
            return True
        current_time = datetime.datetime.now()
        return True if current_time >= expiration_date else False