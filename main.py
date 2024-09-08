import os
from config import Config
from reporting import ReportGenerator
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def main():


    config = Config()

    report_generator = ReportGenerator(config)

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
    total_budget = 2000000

    purchases = report_generator.final_purchases(
        report_generator.report(list_status, date_start, metrics, dimensions, filters, sort, limit, offset, warehouse_type),
        total_budget)

    print("Итоговая закупка:")
    for purchase in purchases:
        print(f"Артикул {purchase[0]}: {purchase[1]} шт., стоимость {purchase[2]:,.2f} руб.")
    print(f"Общая сумма закупки: {sum([p[2] for p in purchases]):,.2f} руб.")

main()