import gspread
import csv
import os
from utils import add_days_to_date_google

class GoogleSheets:
    def __init__(self, config):
        self.gc = gspread.service_account(config.google_sheet_credentials)
        self.output_directory = config.output_directory

    def sheet_selection(self, list_status):
        sh = self.gc.open("24' Закупки")
        data = sh.worksheet('Заказы').get_all_values()
        red_yellow_lists = []
        date_off = []
        non_empty_rows = sum(1 for row in data if row[0])
        for i in range(non_empty_rows):
            if data[i][11] in list_status and data[i][2] not in (None, ''):
                red_yellow_lists.append(data[i][0])
                date_off.append(data[i][2])
        return red_yellow_lists, date_off

    def purchases(self, list_status):
        sh = self.gc.open("24' Закупки")
        red_yellow_lists, date_off = self.sheet_selection(list_status)
        purchases_data = []
        i = 0
        while i < len(red_yellow_lists):
            data = sh.worksheet(red_yellow_lists[i]).get_all_values()
            non_empty_rows = sum(1 for row in data if row[0])
            if red_yellow_lists[i][1] == 'Б':
                if data[non_empty_rows - 1][2]:
                    filtered_data = [[data[i][2], data[i][3], data[i][10]] for i in range(1, non_empty_rows)]
                else:
                    date_on = add_days_to_date_google(date_off[i], 70)
                    filtered_data = [[date_on, data[i][3], data[i][10]] for i in range(1, non_empty_rows)]
            else:
                if data[non_empty_rows - 1][2]:
                    filtered_data = [[data[i][2], data[i][3], data[i][11]] for i in range(1, non_empty_rows)]
                else:
                    date_on = add_days_to_date_google(date_off[i], 35)
                    filtered_data = [[date_on, data[i][3], data[i][11]] for i in range(1, non_empty_rows)]

            purchases_data.extend(filtered_data)

            output_file = os.path.join(self.output_directory, f"{red_yellow_lists[i]}.csv")
            with open(output_file, 'w', newline='', encoding='utf-8-sig') as file:
                writer = csv.writer(file)
                writer.writerows(filtered_data)
            i += 1
        return purchases_data

    def sklad(self):
        sh = self.gc.open("23' Склад")
        data = sh.worksheet("СЧЕТ").get_all_values()
        non_empty_rows = sum(1 for row in data if row[0])
        filtered_data = [[data[i][0], data[i][1]] for i in range(non_empty_rows)]
        output_file = os.path.join(self.output_directory, 'sklad.csv')
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerows(filtered_data)
        return filtered_data

    def marginality(self):
        sh = self.gc.open("24' ОБЩИЕ ПРОДАЖИ")
        data = sh.sheet1.get_all_values()
        non_empty_rows = sum(1 for row in data if row[0])
        filtered_data = [[data[i][0], data[i][13]] for i in range(non_empty_rows)]
        output_file = os.path.join(self.output_directory, 'marginality.csv')
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as file:
            writer = csv.writer(file)
            writer.writerows(filtered_data)
        return filtered_data

    def assortment(self):
        sh = self.gc.open("24' Закупки")
        data = sh.worksheet('Ассортимент').get_all_values()
        non_empty_rows = sum(1 for row in data if row[1])
        filtered_data = [[data[i][1], data[i][3]] for i in range(1, non_empty_rows)]
        offer_id_uniq = [data[i][1] for i in range(1, non_empty_rows)]
        return filtered_data, offer_id_uniq
