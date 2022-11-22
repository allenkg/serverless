from typing import Optional, Type, Any
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
import logging
import os
import shutil
import requests
import boto3
import json
from botocore.exceptions import ClientError

CHROME_OPTIONS = [
    "--headless",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-dev-tools",
    "--no-zygote",
    "--single-process",
    "window-size=2560x1440",
    "--user-data-dir=/tmp/chrome-user-data"
    "--remote-debugging-port=9222"
]
HANDLE_USER_TABLE_NAME = 'Handles'
BASE_URL = 'https://twitter.com/'
BUCKET_NAME = os.environ.get('BUCKET_NAME')
SPINNER_XPATH = '//span'
IMG_XPATH = '//img'


def download_image(image_link, handle) -> None:
    print(image_link, handle)
    res = requests.get(image_link, stream=True)
    image_path = f'/tmp/{handle}.jpg'
    if res.status_code == 200:
        with open(image_path, 'wb') as f:
            shutil.copyfileobj(res.raw, f)
    else:
        print('Image Couldn\'t be retrieved')

def upload_file(name: str, object_name: Optional[Any] = None) -> str:
    file_name = f'/tmp/{name}'
    if object_name is None:
        object_name = os.path.basename(file_name)
    s3_client = boto3.client('s3')
    s3_file_url = '%s/%s/%s' % (s3_client.meta.endpoint_url, BUCKET_NAME, name)
    try:
        response = s3_client.upload_file(file_name, BUCKET_NAME, object_name)
    except ClientError as e:
        logging.error(e)
        raise ValueError('Error upload file to s3')
    return s3_file_url

class SeleniumDriver:
    def __init__(self):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_driver = None

    def init_driver(self):
        self.chrome_options.binary_location = "/opt/chrome/chrome"
        for option in CHROME_OPTIONS:
            self.chrome_options.add_argument(option)
        self.chrome_driver = webdriver.Chrome("/opt/chromedriver", options=self.chrome_options)

    def get_driver(self):
        return self.chrome_driver

    def wait_until_element_exists(self, el_xpath: str) -> WebDriverWait:
        element = None
        try:
            element = WebDriverWait(self.chrome_driver, 10).until(
                EC.presence_of_element_located((By.XPATH, el_xpath))
            )
        finally:
            return element

class HandleRepo:
    def __init__(self, table_name):
        self.table_name = table_name
        self.client = boto3.client('dynamodb')

    def save_item(self, item) -> None:
        self.client.put_item(
            TableName=self.table_name,
            Item=item
        )

    def get_item(self, query) -> dict:
        res = self.client.get_item(
            TableName=self.table_name,
            Key=query
        )
        response = {
            'statusCode': 200,
            'body': json.dumps(res['Item']),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
        }
        return response

    def fetch_all(self, limit: int, last_key: Optional = None) -> dict:
        print('last_key', last_key)
        if last_key:
            data = self.client.scan(
                TableName=self.table_name,
                Limit=limit,
                ExclusiveStartKey=last_key,
            )
        else:
            data = self.client.scan(
                TableName=self.table_name,
                Limit=limit
            )
        response = {
            'statusCode': 200,
            'body': json.dumps(data),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
        }
        return response

class Parser:
    def __init__(self, handle: str) -> None:
        self.base_url = BASE_URL
        self.url = f'{self.base_url}{handle}/photo'
        self.image_download_link = None
        self.selenium_driver = SeleniumDriver()

    def set_image_download_link(self) -> None:
        self.selenium_driver.init_driver()
        driver = self.selenium_driver.get_driver()
        driver.get(self.url)
        spinner_element = self.selenium_driver.wait_until_element_exists(SPINNER_XPATH)
        try:
            img_element = self.selenium_driver.wait_until_element_exists(IMG_XPATH)
        finally:
            img_element_src = img_element.get_property('src')
            if img_element_src:
                self.image_download_link = img_element_src
                print('Successfully find user image download url')

    def get_image_download_link(self) -> str:
        return self.image_download_link


def fetch_all(event: dict) -> dict:
    data = event.get('multiValueQueryStringParameters')
    last_key = None
    limit = 5
    if data:
        last_key = data.get('last')
        limit_key = data.get('limit')
        if last_key:
            last_key = {'handle': {'S': last_key[0]}}
        if limit_key:
            limit = int(limit_key[0])
    repo = HandleRepo(HANDLE_USER_TABLE_NAME)
    return repo.fetch_all(last_key=last_key, limit=limit)


def get_profile_pic(event: dict) -> dict:
    data = event.get('pathParameters')
    query = {
        'handle': {'S': data.get('handle')}
    }
    repo = HandleRepo(HANDLE_USER_TABLE_NAME)
    return repo.get_item(query)


def post(event: dict) -> dict:
    body = event.get('body')
    body = json.loads(body)
    handle = body.get('handle')
    parser = Parser(handle)
    parser.set_image_download_link()
    img_link = parser.get_image_download_link()
    download_image(image_link=img_link, handle=handle)

    s3_file_url = upload_file(f'{handle}.jpg')
    repo = HandleRepo(HANDLE_USER_TABLE_NAME)
    item = {
        'id': {
            'S': handle
        },
        'handle': {
            'S': handle
        },
        'image_url': {
            'S': s3_file_url
        }
    }
    repo.save_item(item)
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "OK"}),
    }


RESOURCE_MAP = {
    '/scrape': post,
    '/users': fetch_all,
    '/user/{handle}/profile_pic': get_profile_pic,
}


def lambda_handler(event, context):
    resource = event['resource']
    handler = RESOURCE_MAP.get(resource)
    return handler(event)
