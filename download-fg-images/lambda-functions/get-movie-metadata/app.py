import os
from io import BytesIO
import json
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlsplit


def get_soup(url):
    return BeautifulSoup(requests.get(url).content,"lxml")

def get_content(soup):
    return soup.find_all("div",{"class":"entry-content"})

def get_metadata(soup):
    return [
        get_attr_text(p) for p in 
        soup.find_all("p",recursive=False) 
        if is_fg_attr(p)
    ]

def is_fg_attr(soup):
    return urlsplit(soup.find("a").attrs.get("href")).netloc == "film-grab.com"

def get_attr_text(soup):
    return soup.text

def get_film_title(soup):
    h1 = soup.find("h1",{"class":"entry-title"})
    if h1: return h1.text
    else: return None

def upload_data(bucket_name: str, file_name: str, data: bytes):
    s3 = boto3.client("s3")
    f = BytesIO(data)
    s3.upload_fileobj(f,bucket_name,file_name)

def make_s3_path(bucket_name: str, file_name: str) -> str:
    return f"s3://{bucket_name}/{file_name}"


def lambda_handler(event, context):
    bucket_name = os.environ["BUCKET_NAME"]

    # Extract request data
    input_data = json.loads(event["body"])

    # Get URL to query parameters
    if "film-url" not in input_data:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "success": False,
                "message": "No `film-url` supplied."
            }) 
        }
    else:
        film_url = input_data["film-url"]
    # Get ID for film
    if "film-id" not in input_data:
        return {
            "statusCode": 200,
            "body": json.dumps({
                "success": False,
                "message": "No `film-id` supplied."
            }) 
        }
    else:
        film_id = input_data["film-id"]
    
    # Get data from HTML
    soup = get_soup(film_url)
    film_title = get_film_title(soup)
    content = get_content(soup)
    film_metadata = get_metadata(content)

    # Format for upload
    file_name = f"{film_id}.json"
    data = {
        "film-id": film_id,
        "film-url": film_url,
        "film-title": film_title,
        "attributes": film_metadata
    }

    # Upload to S3
    upload_data(
        bucket_name, 
        file_name, 
        json.dumps(data).encode()
    )

    # Return the file's new S3 location
    return {
        "statusCode": 200,
        "body": json.dumps({
            "success": True,
            "img-path": make_s3_path(bucket_name, file_name)
        }),
    }
