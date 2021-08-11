#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import json
import logging
from typing import Optional

import requests


class SodaCloud:

    def __init__(self,
                 host: str,
                 port: Optional[str] = None,
                 protocol: Optional[str] = 'https',
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 api_key_id: Optional[str] = None,
                 api_key_secret: Optional[str] = None,
                 token: Optional[str] = None):
        self.host: str = host
        colon_port = f':{port}' if port else ''
        self.api_url: str = f'{protocol}://{self.host}{colon_port}/api'
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.api_key_id: Optional[str] = api_key_id
        self.api_key_secret: Optional[str] = api_key_secret
        self.token: Optional[str] = token

    def dataset(self, dataset_id: str):
        return self.execute_query({
            'type': 'dataset',
            'datasetId': dataset_id
        })

    def tests_with_dependants(self, dataset_id: str):
        return self.execute_query({
            'type': 'testsWithDependants',
            'datasetId': dataset_id
        })

    def execute_command(self, command: dict):
        return self._execute_request('command', command, False)

    def execute_query(self, command: dict):
        return self._execute_request('query', command, False)

    def _execute_request(self, request_type: str, request_body: dict, is_retry: bool):
        logging.debug(
            f'> /api/{request_type} {json.dumps(request_body, indent=2)}')
        request_body['token'] = self.get_token()
        response = requests.post(
            f'{self.api_url}/{request_type}', json=request_body)
        response_json = response.json()
        logging.debug(
            f'< {response.status_code} {json.dumps(response_json, indent=2)}')
        if response.status_code == 401 and not is_retry:
            logging.debug(
                f'Authentication failed. Probably token expired. Reauthenticating...')
            self.token = None
            response_json = self._execute_request(
                request_type, request_body, True)
        else:
            assert response.status_code == 200, f'Request failed with status {response.status_code}: {json.dumps(response_json, indent=2)}'
        return response_json

    def get_token(self):
        if not self.token:
            login_command = {
                'type': 'login'
            }
            if self.api_key_id and self.api_key_secret:
                logging.debug(
                    '> /api/command (login with API key credentials)')
                login_command['apiKeyId'] = self.api_key_id
                login_command['apiKeySecret'] = self.api_key_secret
            elif self.username and self.password:
                logging.debug(
                    '> /api/command (login with username and password)')
                login_command['username'] = self.username
                login_command['password'] = self.password
            else:
                raise RuntimeError(
                    'No authentication in environment variables')

            login_response = requests.post(
                f'{self.api_url}/command', json=login_command)

            if login_response.status_code != 200:
                raise AssertionError(
                    f'< {login_response.status_code} Login failed: {login_response.content}')
            login_response_json = login_response.json()
            self.token = login_response_json.get('token')
            assert self.token, 'No token in login response?!'
            logging.debug('< 200 (login ok, token received)')
        return self.token





#soda_dataset = soda.dataset('e0eddb79-80cd-465d-844b-72e1ad28e151')
#print(soda_dataset)
