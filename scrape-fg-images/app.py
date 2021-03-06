import os
import json
import itertools as it
from pathlib import Path
from typing import List, Dict
from datetime import datetime

import ray

import boto3
import requests
from bs4 import BeautifulSoup


ray.init()


def get_initial_list(url: str) -> List[Dict[str,str]]:
    """Gets a list of movie title/URL pairs
    from the film-grab.com A-Z list.

    :param url: URL for film-grab page with links to individual movie pages
    :returns: List of movie titles and urls to individual movie pages
        has keys {"title","url"}
    """
    # Create page-soup from URL response
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content, "lxml")
    # Find all of the <li> tags in <ul> tag with
    # class `display-posts-listing`
    arr = soup.find(
        "ul",
        attrs={"class": "display-posts-listing"}
    ).find_all("li")
    # Return a dict with the movie title and url
    return [
        {"film-id": i, "title": a.text, "film-url": a.attrs.get("href")} 
        for i, a in enumerate([li.find("a") for li in arr])
    ]

@ray.remote
def get_movie_img_urls(film_data: dict) -> List[dict]:
    """Returns a list of film-still image urls from a 
    specific film's page on film-grab.

    :param url: URL for specific film's page on film-grab
    :returns: List of film-still image dicts on that page
    """
    url = film_data["film-url"]
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content,"lxml")
    return [
        {**film_data, "img-id": i, "img-url": div.find("img").attrs.get("src")} 
        for i, div in enumerate(soup.find_all("div", attrs={"class": "bwg-item"}))
    ]


def download_image(url: str) -> (bytes, str):
    """Downloads an image and returns it in bytes
    as well as the image's content-type.

    :param url: URL for an image
    :returns: An image in bytes and it's content-type
    """
    resp = requests.get(url)
    return resp.content, resp.headers.get('Content-Type')


def get_file_extension(content_type: str) -> str:
    """Gets the file extension for an image based on 
    it's content-type.

    Assumes the content type has format: `image/...`

    :param content_type: The image's content-type from the web
    :returns: The extension of the image
    """
    if content_type is None: return ""
    return "." + content_type.split("/")[-1]


def upload_image(bucket_name: str, filename: str, 
    image_data: bytes):
    """Uploads an image to an S3 bucket.

    Writes the file to the `/tmp` directory, then uploads it
    to S3, then deletes the file.

    :param s3: S3 client object
    :param bucket_name: Name of the bucket to store the image
    :param filename: Name for the file when stored in S3 
        (also temporarily used locally)
    :param image_data: The bytes object with the image's data
    """
    s3 = boto3.client("s3")
    # Save the file locally
    tmp_dir = Path("/tmp")
    img_file = tmp_dir / filename
    img_file.write_bytes(image_data)
    # Upload it to S3
    s3.upload_file(
        str(img_file),
        bucket_name,
        filename
    )
    # Delete the local copy of the file
    img_file.unlink()


def upload_metadata(bucket_name: str, filename: str, 
    data: list):
    """
    """
    s3 = boto3.client("s3")
    # Store the data locally
    tmp_file = Path("./metadata.json")
    tmp_file.write_text(json.dumps(data))
    # Upload it to S3
    s3.upload_file(
        str(tmp_file),
        bucket_name,
        filename
    )


def get_file_id(film_data: dict) -> str:
    return str(film_data["film-id"]) + "." + str(film_data["img-id"])


@ray.remote
def get_img_file_ct(url: str) -> str:
    resp = requests.head(url)
    try: return resp.headers.get("Content-Type")
    except: return None


@ray.remote
def parse_image(IMAGE_BUCKET, image_data: dict):
    img_file, content_type = download_image(image_data["img-url"])
    file_ext = get_file_extension(content_type)
    filename = image_data["filename"] + file_ext
    upload_image(IMAGE_BUCKET, filename, img_file)
    return filename


# Set config consts
START_URL = "https://film-grab.com/movies-a-z"
IMAGE_BUCKET = "apoor-raw-movie-stills"
METADATA_BUCKET = "apoor-movie-still-metadata"

# Scrape the Film Page URLs
print("Getting initial data list...")
fg_data = get_initial_list(START_URL)

# Get img URLs from each individual sites
print("Getting image urls...")
start_time = datetime.now()
futures = [get_movie_img_urls.remote(d) for d in fg_data]
list_of_lists = ray.get(futures)
print("Done.")
print("Time to complete:", datetime.now() - start_time)
print()

# Flatten the list-of-lists
fg_data = list(it.chain(*list_of_lists))

# Get the filenames
fg_data = [{**d, "filename": get_file_id(d)} for d in fg_data]

print(f"Found {len(fg_data)} images.")
print()

print("Checking image content types...")
futures = [get_img_file_ct.remote(d["img-url"]) 
    for d in fg_data]
content_types = ray.get(futures)
print("Done.")
print("Time to complete:", datetime.now() - start_time)
print()

print("Updating filenames...")
fg_data = [{**d, "filename": f"{d['filename']}{get_file_extension(c)}"} 
    for d, c in zip(fg_data, content_types)]
print()

# print("Downloading images...")
# futures = [parse_image.remote(IMAGE_BUCKET,d) 
#     for d in fg_data]
# filenames = ray.get(futures)
# print("Done.")
# print("Time to complete:", datetime.now() - start_time)
# print()

METADATA_FILENAME = "fg-metadata.json"

# Upload the metadata file
print("Uploading metadata...")
upload_metadata(METADATA_BUCKET, METADATA_FILENAME, fg_data)
print("Done.")

