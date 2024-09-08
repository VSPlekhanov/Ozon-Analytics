from datetime import datetime
import requests
import os
import pandas as pd
import json
from http import HTTPStatus
import time
from utils import add_days_to_date_ozon, date_to
import chardet
from datetime import datetime, timedelta


class OzonAPI:
    def __init__(self, config):
        self.client_id = config.get_client_id()
        self.api_key = config.get_ozon_token()

    def goods_request(self, sku):
        url = "https://api-seller.ozon.ru/v2/product/info/list"
        payload = {"offer_id": [], "product_id": [], "sku": sku}
        headers = {"Content-Type": "application/json", "Client-Id": self.client_id, "Api-Key": self.api_key}

        response = requests.post(url, json=payload, headers=headers)
        print("Ответ сервера при запросе об артикулах:", response.status_code)

        if response.status_code == HTTPStatus.OK:
            request_data = response.json()
            df_json = pd.read_json(json.dumps(request_data))
            items = df_json['result'].values[0]
            df = pd.DataFrame([{'id': item['sku'], 'offer_id': item['offer_id']} for item in items])
            return df

        self._handle_errors(response)

    def sales_analytics(self, date_start, metrics, dimensions, filters, sort, limit, offset, output_directory):
        combined_df = pd.DataFrame()
        output_file = os.path.join(output_directory, "sales_analytics_combined.csv")

        if os.path.exists(output_file):
            with open(output_file, 'rb') as f:
                result = chardet.detect(f.read())
                file_encoding = result['encoding']
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
            if datetime.strptime(date_end, "%Y-%m-%d") > datetime.strptime(date_to(), "%Y-%m-%d"):
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
            headers = {"Content-Type": "application/json", "Client-Id": self.client_id, "Api-Key": self.api_key}

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

                df_goods = self.goods_request(df['id'].unique().tolist())

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
                self._handle_errors(response)
                break

        combined_df['date'] = pd.to_datetime(combined_df['date'], format="%Y-%m-%d")
        combined_df = combined_df.sort_values(by='date', ascending=False)

        combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        return combined_df

    def stock_on_warehouses(self, limit, offset, warehouse_type, output_directory):
        url = "https://api-seller.ozon.ru/v2/analytics/stock_on_warehouses"
        payload = {"limit": limit, "offset": offset, "warehouse_type": warehouse_type}
        headers = {"Content-Type": "application/json", "Client-Id": self.client_id, "Api-Key": self.api_key}

        response = requests.post(url, json=payload, headers=headers)
        print("Ответ сервера при запросе об остатках на складе Озона:", response.status_code)

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
                'warehouse_name': item['warehouse_name']
            } for item in rows])
            output_file = os.path.join(output_directory, "stock_on_warehouses.csv")
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            return df

        self._handle_errors(response)

    def calculate_stock_summary_ozon(self, df):
        df_summary = df.groupby('item_code').agg({
            'free_to_sell_amount': 'sum',
            'promised_amount': 'sum'
        }).reset_index()
        df_summary['total_amount'] = df_summary['free_to_sell_amount'] + df_summary['promised_amount']
        return df_summary


    def _handle_errors(self, response):
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
