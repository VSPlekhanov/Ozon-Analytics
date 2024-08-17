import requests
import os
from http import HTTPStatus
import json
import pandas as pd
import warnings
import time
import gspread
import csv
from datetime import datetime, timedelta
import chardet  # Добавляем библиотеку chardet для автоматического определения кодировки

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, 'config/config.json')

with open(config_path, 'r') as f:
    config = json.load(f)

ozon_api_key_file = os.path.join(base_dir, config['ozon_api_key_file'])
client_id_file = os.path.join(base_dir, config['client_id_file'])
google_sheet_credentials = os.path.join(base_dir, config['google_sheet_credentials'])
output_directory = os.path.join(base_dir, config['output_directory'])

with open(ozon_api_key_file, 'r') as f:
    oz_token = f.read().strip()
with open(client_id_file, 'r') as f:
    client_id = f.read().strip()

gc = gspread.service_account(google_sheet_credentials)

def date_to():
    return datetime.today().strftime("%Y-%m-%d")

def add_days_to_date_ozon(date_str, days_to_add):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    new_date = date + timedelta(days=days_to_add)
    return new_date.strftime("%Y-%m-%d")

def add_days_to_date(date_str, days_to_add):
    date = datetime.strptime(date_str, "%d/%m/%Y")
    new_date = date + timedelta(days=days_to_add)
    return new_date.strftime("%d/%m/%Y")

def goods_request(sku):
    url = "https://api-seller.ozon.ru/v2/product/info/list"

    payload = {
        "offer_id": [],
        "product_id": [],
        "sku": sku
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
        items = df_json['result'].values[0]

        df = pd.DataFrame([{
            'id': item['sku'],
            'offer_id': item['offer_id']
        } for item in items])
        return df

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

def sales_analytics(date_start, metrics, dimensions, filters, sort, limit, offset):
    combined_df = pd.DataFrame()
    output_file = os.path.join(output_directory, "sales_analytics_combined.csv")

    # Проверка существования файла и загрузка старых данных
    if os.path.exists(output_file):
        # Определяем кодировку файла
        with open(output_file, 'rb') as f:
            result = chardet.detect(f.read())
            file_encoding = result['encoding']

        # Чтение файла с использованием определенной кодировки
        try:
            combined_df = pd.read_csv(output_file, encoding=file_encoding)
            if not combined_df.empty:
                last_date = combined_df['date'].max()
                date_start = (pd.to_datetime(last_date) + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                combined_df = pd.DataFrame()
        except Exception as e:
            print(f"Ошибка при чтении файла: {e}")
            combined_df = pd.DataFrame()
    else:
        combined_df = pd.DataFrame()

    while True:
        date_end = add_days_to_date_ozon(date_start, 7)
        if datetime.strptime(date_end, "%Y-%m-%d") > datetime.today():
            date_end = date_to()

        url = "https://api-seller.ozon.ru/v1/analytics/data"
        payload = {
            "date_from": date_start,
            "date_to": date_end,
            "metrics": metrics,
            "dimension": dimensions,
            "filters": filters,
            "sort": sort,
            "limit": limit,
            "offset": offset
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
                'revenue': item['metrics'][0],
                'ordered_units': item['metrics'][1],
                'price': item['metrics'][0] / item['metrics'][1] if item['metrics'][1] != 0 else 0
            } for item in data])

            id_uniq = df['id'].unique()
            id_uniq = id_uniq.tolist()
            time.sleep(60)
            df_goods = goods_request(id_uniq)

            df['id'] = df['id'].astype(int)
            result_df = pd.merge(df_goods, df, on='id', how='left')

            combined_df = pd.concat([combined_df, result_df], ignore_index=True)

            if len(data) < limit:
                offset = 0
                date_start = add_days_to_date_ozon(date_end, 1)
            else:
                offset += limit

            time.sleep(60)

            if datetime.strptime(date_start, "%Y-%m-%d") > datetime.today():
                break

        elif response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            print('Превышен лимит на запросы в 1 мин.')
            time.sleep(60)
        else:
            if response.status_code == HTTPStatus.BAD_REQUEST:
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
            break

    # Преобразование даты и сортировка
    combined_df['date'] = pd.to_datetime(combined_df['date'], format="%Y-%m-%d")
    combined_df = combined_df.sort_values(by='date', ascending=False)

    # Сохранение файла с обновленными данными
    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')

def stock_on_warehouses(limit, offset, warehouse_type):
    url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"

    payload = {
        "limit": limit,
        "offset": offset,
        "warehouse_type": warehouse_type
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
        output_file = os.path.join(output_directory, "stock_on_warehouses.csv")
        df.to_csv(output_file, index=False, encoding='utf-8-sig')

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
    

def sheet_selection(list_status):
    sh = gc.open("24' Закупки")
    data = sh.worksheet('Заказы').get_all_values()
    red_yellow_lists = []
    date_off = []
    non_empty_rows = sum(1 for row in data if row[0])
    for i in range(non_empty_rows):
        if data[i][11] in list_status:
            red_yellow_lists.append(data[i][0])
            date_off.append(data[i][2])
    return red_yellow_lists, date_off

def purchases(list_status):
    sh = gc.open("24' Закупки")
    red_yellow_lists, date_off = sheet_selection(list_status)
    i = 0
    while i < len(red_yellow_lists):
        data = sh.worksheet(red_yellow_lists[i]).get_all_values()
        non_empty_rows = sum(1 for row in data if row[0])
        if red_yellow_lists[i][1] =='Б':
            if data[non_empty_rows - 1][2]:
                filtered_data = [[data[i][2], data[i][3], data[i][10]] for i in range(non_empty_rows)]
            else:
                date_on = add_days_to_date(date_off[i], 70)
                filtered_data = [[date_on, data[i][3], data[i][10]] for i in range(non_empty_rows)]
                filtered_data[0][0] = data[0][2]
        else:
            if data[non_empty_rows - 1][2]:
                filtered_data = [[data[i][2], data[i][3], data[i][11]] for i in range(non_empty_rows)]
            else:
                date_on = add_days_to_date(date_off[i], 35)
                filtered_data = [[date_on, data[i][3], data[i][11]] for i in range(non_empty_rows)]
                filtered_data[0][0] = data[0][2]

        output_file = os.path.join(output_directory, f"{red_yellow_lists[i]}.csv")
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerows(filtered_data)
        i += 1

def sklad():
    sh = gc.open("23' Склад")
    data = sh.sheet1.get_all_values()
    output_file = os.path.join(output_directory, 'sklad.csv')
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerows(data)

def marginality():
    sh = gc.open("24' ОБЩИЕ ПРОДАЖИ")
    data = sh.sheet1.get_all_values()
    non_empty_rows = sum(1 for row in data if row[0])
    filtered_data = [[data[i][0], data[i][13]] for i in range(non_empty_rows)]
    output_file = os.path.join(output_directory, 'marginality.csv')
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)
        writer.writerows(filtered_data)

list_status = {
    'Полностью вышел',
    'Частично на складе в К.',
    'Оплату внесла',
    'Выкуплен',
    'На складе в К.'
}
date_start = '2022-05-01'
metrics = ['revenue', 'ordered_units']
dimensions = ['sku', 'day']
filters = []
limit = 1000
offset = 0
sort = [{"key": 'day', "order": 'DESC'}]
warehouse_type = 'ALL'


sales_analytics(date_start, metrics, dimensions, filters, sort, limit, offset)
stock_on_warehouses(limit, offset, warehouse_type)
purchases(list_status)
sklad()
marginality()
