import pandas as pd
import requests
from io import StringIO

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}
source = requests.get('https://en.wikipedia.org/wiki/List_of_ISO_639_language_codes', headers=headers)

if source.status_code != 200:
    raise Exception('Failed to load page {}'.format(source.status_code))
else:
    df = pd.read_html(StringIO(source.text))[0]
    df.columns = ['_'.join(set(col)).strip() for col in df.columns.values]
    df.to_csv('./language_codes.tsv', sep='\t', index=False)