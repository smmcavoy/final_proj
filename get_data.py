import requests

imdb_files = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz"
]

for file in imdb_files:
    save_file = f"/Volumes/workspace/imdb/imdb_data/compressed/{file}"
    with requests.get(f"https://datasets.imdbws.com/{file}", stream=True) as r:
        with open(save_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)