import boto3
import json

def create_or_get_bucket(bucket_name):
    s3 = boto3.client('s3', endpoint_url="http://localstack:4566")
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

