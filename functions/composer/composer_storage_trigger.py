import requests
from google.auth.transport.requests import Request
from google.oauth2._client import _JWT_GRANT_TYPE, _token_endpoint_request
from google.oauth2.service_account import Credentials

if __name__ == '__main__':
    client_id = '41454429176-rujv295r9sl8s4lqomm4dbi6rtmp417b.apps.googleusercontent.com'
    oauth_token_uri = 'https://www.googleapis.com/oauth2/v4/token'
    web_server_id = 'z36d563287ddd8650-tp'  # Airflow Web Server = https://<web_server_id>.appspot.com

    service_account_credentials_path = './kr-co-vcnc-tada-7cb37e11e075.json'
    dag_name = 'dag_server_log_parquet'
    data = {'conf': {'date_kr': '2019-11-24'}}

    # service account credentials 파일로 bootstrap credentials 을 생성합니다.
    bootstrap_credentials = Credentials.from_service_account_file(service_account_credentials_path)
    signer_email = bootstrap_credentials.service_account_email
    signer = bootstrap_credentials.signer

    # OAuth 2.0 service account credentials 을 생성합니다.
    # token_uri 값을 바꾸고, additional_claims 을 추가합니다.
    service_account_credentials = Credentials(signer, signer_email, oauth_token_uri,
                                              additional_claims={'target_audience': client_id})

    # OpenID Connect token 을 획득합니다.
    service_account_jwt = service_account_credentials._make_authorization_grant_assertion()
    body = {'assertion': service_account_jwt, 'grant_type': _JWT_GRANT_TYPE}
    token_response = _token_endpoint_request(Request(), oauth_token_uri, body)
    google_open_id_connect_token = token_response['id_token']

    # 획득한 token 을 HTTP Header 에 담아서, Airflow Web Server 의 REST API 를 호출합니다.
    resp = requests.request('POST',
                            f'https://{web_server_id}.appspot.com/api/experimental/dags/{dag_name}/dag_runs',
                            headers={'Authorization': f'Bearer {google_open_id_connect_token}'},
                            json=data)

    if resp.status_code == 403:
        raise Exception(f'Service account {signer_email} does not have permission to '
                        f'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(f'Bad response from application: {resp.status_code} / {resp.headers} / {resp.text}')
