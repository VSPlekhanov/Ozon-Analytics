import os
import pandas as pd
import time
from datetime import datetime
from google_sheets import GoogleSheets
from ozon_api import OzonAPI


class ReportGenerator:
    def __init__(self, config):
        self.config = config
        self.google_sheets = GoogleSheets(config)
        self.ozon_api = OzonAPI(config)

    def _read_key(self, key_file):
        with open(key_file, 'r') as f:
            return f.read().strip()

    def report(self, list_status, date_start, metrics, dimensions, filters, sort, limit, offset, warehouse_type):
        assortment_data, offer_id_uniq = self.google_sheets.assortment()
        marginality_data = self.google_sheets.marginality()
        sklad_data = self.google_sheets.sklad()
        purchases_data = self.google_sheets.purchases(list_status)

        df_sales_analytics = self.ozon_api.sales_analytics(date_start, metrics, dimensions, filters, sort, limit, offset, self.config.output_directory)
        time.sleep(60)
        df_stock_on_ozon = self.ozon_api.stock_on_warehouses(limit, offset, warehouse_type, self.config.output_directory)

        df_report = pd.DataFrame(columns=['sku', 'offer_id', 'name', 'price', 'marginality', 'weeks_remaining', 'required_order'])

        marginality_dict = {row[0]: row[1] for row in marginality_data}
        name_dict = {row[0]: row[1] for row in assortment_data}
        sklad_dict = {row[0]: row[1] for row in sklad_data}

        current_week = datetime.now().isocalendar()[1]

        purchases_dict = {}
        for row in purchases_data:
            date_on = datetime.strptime(row[0], "%d/%m/%Y").isocalendar()[1]
            offer_id = row[1]
            amount = int(row[2].replace(',', ''))
            if offer_id not in purchases_dict:
                purchases_dict[offer_id] = []
            purchases_dict[offer_id].append((date_on, amount))

        forecasting_file = os.path.join(self.config.output_directory, 'forecasting.csv')
        df_forecasting = pd.read_csv(forecasting_file)

        for offer_id in offer_id_uniq:
            row_forecasting = df_forecasting[df_forecasting['offer_id'] == int(offer_id)].iloc[0, 1:50].values
            row_sales_analytics = df_sales_analytics[df_sales_analytics['offer_id'] == offer_id]
            row_stock_on_ozon = df_stock_on_ozon[df_stock_on_ozon['item_code'] == offer_id]

            prices = row_sales_analytics['price'].values
            valid_prices = [price for price in prices if price != 0]
            if valid_prices:
                result_price = round(sum(valid_prices) / len(valid_prices))
            else:
                result_price = 0

            if not row_stock_on_ozon.empty:
                sklad_value = sklad_dict.get(offer_id)
                if sklad_value is not None:
                    result_stock = int(sklad_value) + row_stock_on_ozon['total_amount'].values[0]
                else:
                    result_stock = row_stock_on_ozon['total_amount'].values[0]
            else:
                sklad_value = sklad_dict.get(offer_id)
                if sklad_value is not None:
                    result_stock = int(sklad_value)
                else:
                    result_stock = 0

            weeks_remaining = 0
            i = current_week

            if offer_id in purchases_dict:
                while result_stock > 0 or len(purchases_dict[offer_id]) > 0:
                    for date_on, amount in purchases_dict[offer_id]:
                        if date_on == i:
                            result_stock += amount
                            purchases_dict[offer_id].remove((date_on, amount))
                    if result_stock - row_forecasting[i] >= 0:
                        weeks_remaining += 1
                        result_stock -= row_forecasting[i]
                    else:
                        if len(purchases_dict[offer_id]) > 0:
                            weeks_remaining += 1
                        else:
                            break
                    i = (i + 1) % 48
            else:
                while result_stock > 0:
                    if result_stock - row_forecasting[i] < 0:
                        break
                    else:
                        weeks_remaining += 1
                        result_stock -= row_forecasting[i]
                    i = (i + 1) % 48

            if weeks_remaining >= 24:
                required_order = 0
            else:
                remaining_weeks = 24 - weeks_remaining
                forecast_for_remaining_weeks = 0
                for week in range(remaining_weeks):
                    forecast_week_index = (i + week) % len(row_forecasting)
                    forecast_for_remaining_weeks += row_forecasting[forecast_week_index]
                required_order = max(forecast_for_remaining_weeks - result_stock, 0)

            if not row_sales_analytics.empty:
                df_report = df_report._append({
                    'sku': row_sales_analytics['id'].values[0],
                    'offer_id': offer_id,
                    'name': name_dict.get(offer_id),
                    'price': result_price,
                    'marginality': marginality_dict.get(offer_id),
                    'weeks_remaining': weeks_remaining,
                    'required_order': required_order
                }, ignore_index=True)
            else:
                df_report = df_report._append({
                    'sku': None,
                    'offer_id': offer_id,
                    'name': name_dict.get(offer_id),
                    'price': 0,
                    'marginality': marginality_dict.get(offer_id),
                    'weeks_remaining': weeks_remaining,
                    'required_order': required_order
                }, ignore_index=True)

        df_report['marginality'] = df_report['marginality'].str.rstrip('%').astype('float')
        df_report = df_report.sort_values(by='marginality', ascending=False).reset_index(drop=True)
        return df_report

    def final_purchases(self, df_report, total_budget):
        purchases = []
        remaining_budget = total_budget

        for index, row in df_report.iterrows():
            offer_id = row['offer_id']
            price = row['price']
            required_order = row['required_order']
            if price != 0:
                if remaining_budget > 0 and required_order > 0:
                    purchase_cost = required_order * price
                    if purchase_cost <= remaining_budget:
                        remaining_budget -= purchase_cost
                        purchases.append((offer_id, required_order, purchase_cost))
                    else:
                        possible_quantity = remaining_budget // price
                        if possible_quantity > 0:
                            purchase_cost = possible_quantity * price
                            remaining_budget -= purchase_cost
                            purchases.append((offer_id, possible_quantity, purchase_cost))
        return purchases