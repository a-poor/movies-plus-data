import os
import json
import itertools as it
from pathlib import Path
from typing import List, Dict
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor
# import ray

import boto3
import requests
from bs4 import BeautifulSoup


# ray.init()


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
    return "." + content_type.split("/")[-1]


def upload_image(s3: 'botocore.client.S3', bucket_name: str, filename: str, 
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


def upload_metadata(s3: 'botocore.client.S3', bucket_name: str, filename: str, 
    data: list):
    """
    """
    # Store the data locally
    tmp_file = Path("./metadata.json")
    tmp_file.write_text(json.dumps(data))
    # Upload it to S3
    s3.upload_file(
        str(tmp_file),
        bucket_name,
        filename
    )


def get_filename(film_data: dict) -> str:
    return str(film_data["film-id"]) + "." + str(film_data["img-id"])+".jpeg"


def parse_image(s3, IMAGE_BUCKET, image_data: dict):
    img_file, content_type = download_image(image_data["img-url"])
    # image_data["filename"] += get_file_extension(content_type)
    filename = image_data["filename"]
    upload_image(s3, IMAGE_BUCKET, filename, img_file)
    return content_type


def run():
    """
    """
    # Get config settings from env vars
    # START_URL = os.environ["FG_START_URL"]
    # IMAGE_BUCKET = os.environ["RAW_IMG_BUCKET"]
    # METADATA_BUCKET = os.environ["METADATA_BUCKET"]

    START_URL = "https://film-grab.com/movies-a-z"
    IMAGE_BUCKET = "apoor-raw-movie-stills"
    METADATA_BUCKET = "apoor-movie-still-metadata"

    METADATA_FILENAME = "fg-scrape-metadata.json"

    # Connect to S3
    print("Connecting to s3...")
    s3 = boto3.client("s3")

    # Scrape the Film Page URLs
    print("Getting initial data list...")
    fg_data = get_initial_list(START_URL)

    # Get img URLs from each individual sites
    print("Getting image urls...")
    start_time = datetime.now()
    with ThreadPoolExecutor() as P:
        fg_data = list(it.chain(*P.map(
            get_movie_img_urls, fg_data
        )))
    # list_of_lists = [get_movie_img_urls.remote(d) for d in fg_data]

    # Flatten the list-of-lists

    # ...

    print("Done.")
    print("Time to complete:", datetime.now() - start_time)
    print()

    # Get the filenames
    fg_data = [{**d, "filename": get_filename(d)} for d in fg_data]

    # print("Downloading images...")
    # print("Getting image urls...")
    # with ThreadPoolExecutor() as P:
    #     filenames = list(P.map(
    #         lambda d: parse_image(s3, IMAGE_BUCKET, filename, d),
    #         fg_data
    #     )) # Will this return types in order?
    # print("Done.")
    # print("Time to complete:", datetime.now() - start_time)
    # print()


    upload_metadata(s3, METADATA_BUCKET, METADATA_FILENAME, fg_data)


    ##############################################################

    # # For each movie, get the image URLs
    # for i, film in enumerate(fg_data):
    #     # Scrape the list of images on the film's page
    #     img_urls = get_movie_img_urls(film["film-url"])
    #     fg_data[i]["images"] = [{"url": url} for url in img_urls]

    #     jlen = len(str(len(img_urls)))

    #     # For each image... upload it to S3 and store the filename
    #     for j, url in enumerate(img_urls):
    #         # Download it...
    #         img_data, content_type = download_image(url)

    #         # Set the filename
    #         filename = f"{i:0{ilen}d}.{j:0{jlen}d}" + get_file_extension(content_type)
    #         fg_data[i]["images"][j]["filename"] = filename

    #         # Upload it to S3
    #         upload_image(s3, IMAGE_BUCKET, filename, img_data)
    
    # # Store the metadata in a bucket
    # upload_metadata(s3, METADATA_BUCKET, METADATA_FILENAME, fg_data)


if __name__ == '__main__': run()
