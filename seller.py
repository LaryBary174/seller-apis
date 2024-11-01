import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров на Ozon по указанному last_id.

    Args:
        last_id (str): Последний ID товара.
        client_id (str): ID клиента Ozon.
        seller_token (str): Токен доступа к API.

    Returns:
        dict: Объект с данными о продуктах.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает список артикулов товаров на Ozon.

    Args:
        client_id (str): ID клиента Ozon.
        seller_token (str): Токен доступа к API.

    Returns:
        list: Список артикулов товаров.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров на Ozon.

    Args:
        prices (list): Список цен на товары.
        client_id (str): ID клиента Ozon.
        seller_token (str): Токен доступа к API.

    Returns:
        dict: Ответ от API.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет информацию о наличии товаров на складе Ozon.

    Args:
        stocks (list): Список остатков товаров.
        client_id (str): ID клиента Ozon.
        seller_token (str): Токен доступа к API.

    Returns:
        dict: Ответ от API.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает файл остатков товаров с сайта Casio и извлекает его.

    Returns:
        list: Список остатков товаров.
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
        Создает список остатков товаров на складе Ozon.

        Args:
            watch_remnants (list): Список остатков часов.
            offer_ids (list): Список артикулов товаров.

        Returns:
            list: Список остатков товаров в нужном формате для API.
        """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
        Создает список цен на товары на Ozon.

        Args:
            watch_remnants (list): Список остатков часов.
            offer_ids (list): Список артикулов товаров.

        Returns:
            list: Список цен на товары в нужном формате для API.
        """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразует цену из формата строки к числовому виду.

    Args:
        price (str): Цена в формате строки. Пример: "5'990.00 руб."

    Returns:
        str: Цена в числовом формате. Пример: "5990".
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Делит список на части по n элементов.

    Args:
        lst (list): Исходный список.
        n (int): Количество элементов в каждой части.

    Yields:
        list: Части исходного списка.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
        Асинхронное обновление цен на товары на Ozon.

        Args:
            watch_remnants (list): Список остатков часов.
            client_id (str): ID клиента Ozon.
            seller_token (str): Токен доступа к API.

        Returns:
            list: Список обновленных цен.
        """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
        Асинхронное обновление информации о наличии товаров на складе Ozon.

        Args:
            watch_remnants (list): Список остатков часов.
            client_id (str): ID клиента Ozon.
            seller_token (str): Токен доступа к API.

        Returns:
            tuple: Список остатков товаров, включающих только те, у которых остаток > 0, и полный список остатков.
        """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
        Главная функция, выполняющая обновление остатков и цен для Ozon.
        """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()