import requests


client_id = '172709f91e5747828bc4863e00745e6a'
client_secret = '27d2da0ef78e42cda330155464bbca36'
code = '4019560'

data = {
    'grant_type': 'authorization_code',
    'code': code,
    'client_id': client_id,
    'client_secret': client_secret
}

response = requests.post('https://oauth.yandex.ru/token', data=data)
print(response.json())
