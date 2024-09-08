from datetime import datetime, timedelta


def date_to():
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def add_days_to_date_ozon(date_str, days_to_add):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    new_date = date + timedelta(days=days_to_add)
    return new_date.strftime("%Y-%m-%d")


def add_days_to_date_google(date_str, days_to_add):
    date = datetime.strptime(date_str, "%d/%m/%Y")
    new_date = date + timedelta(days=days_to_add)
    return new_date.strftime("%d/%m/%Y")
