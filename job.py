import requests

# Ваш токен доступа
access_token = 'y0_AgAAAAB3sIozAAw5MwAAAAEM01-PAADB81Zu49JCEZpHQQjNUjRRCad2sA'

# URL для запроса
url = 'https://api.direct.yandex.com/json/v5/clients'

# Заголовки запроса
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept-Language': 'ru',
    'processingMode': 'auto',
    'returnMoneyInMicros': 'false',
}

# Параметры запроса
body = {
    "method": "get",
    "params": {
        "FieldNames": ["Login"]
    }
}

# Выполнение запроса
response = requests.post(url, json=body, headers=headers)

# Вывод полного ответа для отладки
print("Полный ответ от API:", response.text)

# Обработка ответа
if response.status_code == 200:
    data = response.json()
    if 'result' in data:
        logins = [client['Login'] for client in data['result']['Clients']]
        print("Логины клиентов:", logins)
    else:
        print("Ошибка: ключ 'result' отсутствует в ответе", data)
else:
    print(f"Ошибка: {response.status_code}, {response.text}")
