import requests
import json
from bs4 import BeautifulSoup

url = "https://www.imdb.com/chart/top/"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, "html.parser")

movie_links = soup.find_all("a", class_="ipc-title-link-wrapper")
movie_ids = []

for link in movie_links:
    movie_id = link["href"].split("/")[2]
    movie_ids.append(movie_id)

filtered_movie_ids = movie_ids[:-7]

print(filtered_movie_ids)

with open("movie_ids.json", "w") as f:
    json.dump(filtered_movie_ids, f)
