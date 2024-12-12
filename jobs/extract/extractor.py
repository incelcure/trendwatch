import requests
import boto3
import botocore
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def create_or_get_bucket(bucket_name):
    s3 = boto3.client('s3')

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Бакет {bucket_name} уже существует")
    except s3.exceptions.ClientError as e:
        print(f"Бакет {bucket_name} не существует. Создание...")
        s3.create_bucket(Bucket=bucket_name)

    return s3



def save_to_s3(data, key, bucket_name):
    s3 = create_or_get_bucket(bucket_name)

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    print(f"Данные сохранены в S3: {key}")


def fetch_data_from_api(api_url, params=None, headers=None):
    response = requests.get(api_url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def fetch_and_store(api_name, endpoint, url, bucket_name, headers=None, params=None):
    try:
        data = fetch_data_from_api(url, params, headers)
        timestamp = datetime.now().strftime('%Y/%m/%d/%H')
        file_name = f"data_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        s3_key = f"raw_data/{api_name}/{endpoint}/{timestamp}/{file_name}"
        save_to_s3(data, s3_key, bucket_name)
    except Exception as e:
        print(f"Ошибка при работе с {url}: {e}")


if __name__ == "__main__":
    params = {"page": 1, "max": 16}
    headers = {"x-api-key": os.environ["MARKET_CAL_API_KEY"], "Accept-encoding": "deflate, gzip",
               "Accept": "application/json"}
    url = "https://developers.coinmarketcal.com/v1/events"
    fetch_and_store("CoinMarketCalAPI", "events", url, "trendwatch", headers, params)

    params = {"lang": "EN", "limit": 10}
    headers = {"x-api-key": os.environ["CRYPTOCOMPARE_API_KEY"], "Content-type": "application/json; charset=UTF-8"}
    url = "https://data-api.cryptocompare.com/news/v1/article/list"
    fetch_and_store("CryptoCompareAPI", "news/v1/article/list", url, "trendwatch", headers, params)