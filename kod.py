import gspread
import csv

gc = gspread.service_account("/Users/mishka.bashkirka/Downloads/ozon-project-431409-8e8d8dac7566.json")

def zakupki(sheet, list, link):
    sh = gc.open(sheet)
    i = 0
    while i < len(list):
        data = sh.worksheet(list[i]).get_all_values()
        non_empty_count = sum(1 for row in data if row[0])
        if list[i][1] == 'Б':
            filtered_data = [[data[i][2], data[i][3], data[i][10]] for i in range(non_empty_count)]
        else:
            filtered_data = [[data[i][2], data[i][3], data[i][11]] for i in range(non_empty_count)]
        with open('/Users/mishka.bashkirka/Desktop/' + link[i] + '.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(filtered_data)
        i += 1

def sklad(sheet):
    sh = gc.open(sheet)
    data = sh.sheet1.get_all_values()
    with open('/Users/mishka.bashkirka/Desktop/sklad.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)

def marginality(sheet):
    sh = gc.open(sheet)
    data = sh.sheet1.get_all_values()
    with open('/Users/mishka.bashkirka/Desktop/marginality.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)


sheet = "24' Закупки"
list = ['ЗБ10', 'ЗЧ17']
link = ['ЗБ10', 'ЗЧ17']
result = zakupki(sheet, list, link)
sklad1 = "23' Склад"
res = sklad(sklad1)