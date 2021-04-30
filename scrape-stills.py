
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

START_URL = "https://film-grab.com/movies-a-z"


def get_initial_list(url: str) -> List[Dict[str,str]]:
    resp = requests.get(url)
    soup = BeautifulSoup(resp.content,"lxml")
    arr = soup.find(
        "ul",
        attrs={"class":"display-posts-listing"}
    ).find_all("li")
    return [
        {"title": a.text, "url": a} 
        for a in [li.find("a") for a in arr]
    ]


def get_movie_img_urls(url: str) -> List[str]:
    pass


def download_image(url: str) -> bytes:
    pass


def run():
    pass

if __name__ == '__main__':
    run()

