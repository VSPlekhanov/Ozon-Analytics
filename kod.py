import requests
from http import HTTPStatus
import json
import pandas as pd
import warnings
import time
import gspread
import csv
from datetime import datetime, timedelta

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

def add_days_to_date(date_str, days_to_add):
    date = datetime.strptime(date_str, "%d/%m/%Y")
    new_date = date + timedelta(days=days_to_add)
    return new_date.strftime("%d/%m/%Y")

with open("/Users/mishka.bashkirka/Downloads/api-key.txt", 'r') as f:
    oz_token = f.read()
with open("/Users/mishka.bashkirka/Downloads/client-id.txt", 'r') as f:
    client_id = f.read()


url = "https://api-seller.ozon.ru/v1/analytics/data"
payload = {
    "date_from": '2024-01-01',
    "date_to": '2024-07-30',
    "metrics": ['ordered_units'],
    "dimension": ['sku', 'day'],
    "filters": [],
    "sort": [{
            "key": 'day',
            "order": 'DESC'
    }],
    "limit": 1000,
    "offset": 0
}
headers = {
    "Content-Type": "application/json",
    "Client-Id": client_id,
    "Api-Key": oz_token
}
response = requests.post(url, json=payload, headers=headers)
print("Ответ сервера при запросе о продажах:", response.status_code)
if response.status_code == HTTPStatus.OK:
    request_data = response.json()
    df_json = pd.read_json(json.dumps(request_data))
    data = df_json['result'].values[0]
    df = pd.DataFrame([{
        'id': item['dimensions'][0]['id'],
        'name': item['dimensions'][0]['name'],
        'date': item['dimensions'][1]['id'],
        'ordered_units': item['metrics'][0]
    } for item in data])
    df.to_csv("/Users/mishka.bashkirka/Desktop/select_sales_info.csv")
elif response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
    print('Превышен лимит на запросы в 1 мин.')
elif response.status_code == HTTPStatus.BAD_REQUEST:
    print('Неверный параметр.')
elif response.status_code == HTTPStatus.FORBIDDEN:
    print('Доступ запрещён.')
elif response.status_code == HTTPStatus.NOT_FOUND:
    print('Ответ не найден.')
elif response.status_code == HTTPStatus.CONFLICT:
    print('Конфликт запроса.')
elif response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
    print('Внутренняя ошибка сервера.')
else:
    print(f"Что-то пошло не так! Ошибка отличная от 400, 403, 404, 409, 429, 500.")

time.sleep(65)

url = "https://api-seller.ozon.ru/v1/analytics/data"
payload = {
    "date_from": '2024-01-01',
    "date_to": '2024-07-30',
    "metrics": ['ordered_units'],
    "dimension": ['sku', 'day', 'category3'],
    "filters": [],
    "sort": [{
            "key": 'day',
            "order": 'DESC'
    }],
    "limit": 1000,
    "offset": 0
}
headers = {
    "Content-Type": "application/json",
    "Client-Id": client_id,
    "Api-Key": oz_token
}
response = requests.post(url, json=payload, headers=headers)
print("Ответ сервера при запросе о продажах:", response.status_code)
if response.status_code == HTTPStatus.OK:
    request_data = response.json()
    df_json = pd.read_json(json.dumps(request_data))
    data = df_json['result'].values[0]
    df = pd.DataFrame([{
        'id': item['dimensions'][0]['id'],
        'name': item['dimensions'][0]['name'],
        'date': item['dimensions'][1]['id'],
        'ordered_units': item['metrics'][0]
    } for item in data])
    df.to_csv("/Users/mishka.bashkirka/Desktop/select_sales_info_3_level.csv")
elif response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
    print('Превышен лимит на запросы в 1 мин.')
elif response.status_code == HTTPStatus.BAD_REQUEST:
    print('Неверный параметр.')
elif response.status_code == HTTPStatus.FORBIDDEN:
    print('Доступ запрещён.')
elif response.status_code == HTTPStatus.NOT_FOUND:
    print('Ответ не найден.')
elif response.status_code == HTTPStatus.CONFLICT:
    print('Конфликт запроса.')
elif response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
    print('Внутренняя ошибка сервера.')
else:
    print(f"Что-то пошло не так! Ошибка отличная от 400, 403, 404, 409, 429, 500.")

time.sleep(65)

url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
payload = {
    "limit": 1000,
    "offset": 0,
    "warehouse_type": 'ALL'
}
headers = {
    "Content-Type": "application/json",
    "Client-Id": client_id,
    "Api-Key": oz_token
}
response = requests.post(url, json=payload, headers=headers)
print("Ответ сервера при запросе о продажах:", response.status_code)
if response.status_code == HTTPStatus.OK:
    request_data = response.json()
    df_json = pd.read_json(json.dumps(request_data))
    rows = df_json['result'].values[0]
    df = pd.DataFrame([{
        'sku': item['sku'],
        'item_code': item['item_code'],
        'item_name': item['item_name'],
        'free_to_sell_amount': item['free_to_sell_amount'],
        'promised_amount': item['promised_amount'],
        'reserved_amount': item['reserved_amount'],
        'warehouse_name': item['warehouse_name']
    } for item in rows])
    df.to_csv("/Users/mishka.bashkirka/Desktop/stock_on_warehouses.csv")
elif response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
    print('Превышен лимит на запросы в 1 мин.')
elif response.status_code == HTTPStatus.BAD_REQUEST:
    print('Неверный параметр.')
elif response.status_code == HTTPStatus.FORBIDDEN:
    print('Доступ запрещён.')
elif response.status_code == HTTPStatus.NOT_FOUND:
    print('Ответ не найден.')
elif response.status_code == HTTPStatus.CONFLICT:
    print('Конфликт запроса.')
elif response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
    print('Внутренняя ошибка сервера.')
else:
    print(f"Что-то пошло не так! Ошибка отличная от 400, 403, 404, 409, 429, 500.")


gc = gspread.service_account("/Users/mishka.bashkirka/Downloads/ozon-project-431409-8e8d8dac7566.json")

sh = gc.open("24' Закупки")
data = sh.worksheet('Заказы').get_all_values()
red_yellow_lists = []
date_off = []
red_lists = []
non_empty_count = sum(1 for row in data if row[0])
for i in range(non_empty_count):
    if data[i][11] == 'Полностью вышел' or data[i][11] == 'Частично на складе в К.' or data[i][11] == 'Оплату внесла' or data[i][11] == 'Выкуплен' or data[i][11] == 'На складе в К.':
        red_yellow_lists.append(data[i][0])
        if data[i][11] == 'Частично на складе в К.' or data[i][11] == 'Оплату внесла' or data[i][11] == 'Выкуплен' or data[i][11] == 'На складе в К.':
            red_lists.append(data[i][0])
            date_off.append(data[i][2])
i = 0
while i < len(red_yellow_lists):
    data = sh.worksheet(red_yellow_lists[i]).get_all_values()
    non_empty_count = sum(1 for row in data if row[0])
    if red_yellow_lists[i][1] == 'Б':
        if red_yellow_lists[i] in red_lists:
            date_on = add_days_to_date(date_off[i - len(red_yellow_lists) + len(red_lists)], 70)
            filtered_data = [[date_on, data[i][3], data[i][10]] for i in range(non_empty_count)]
        else:
            filtered_data = [[data[i][2], data[i][3], data[i][10]] for i in range(non_empty_count)]
    else:
        if red_yellow_lists[i] in red_lists:
            date_on = add_days_to_date(date_off[i - len(red_yellow_lists) + len(red_lists)], 35)
            filtered_data = [[date_on, data[i][3], data[i][11]] for i in range(non_empty_count)]
        else:
            filtered_data = [[data[i][2], data[i][3], data[i][11]] for i in range(non_empty_count)]
    with open('/Users/mishka.bashkirka/Desktop/' + red_yellow_lists[i] + '.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(filtered_data)
    i += 1

sh = gc.open("23' Склад")
data = sh.sheet1.get_all_values()
with open('/Users/mishka.bashkirka/Desktop/sklad.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(data)

sh = gc.open("24' ОБЩИЕ ПРОДАЖИ")
data = sh.sheet1.get_all_values()
non_empty_count = sum(1 for row in data if row[0])
filtered_data = [[data[i][0], data[i][13]] for i in range(non_empty_count)]
with open('/Users/mishka.bashkirka/Desktop/marginality.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(filtered_data)
