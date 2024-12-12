import requests
from jobs.extract.s3_storage import save_to_s3
from datetime import datetime

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