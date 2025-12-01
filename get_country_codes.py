from bs4 import BeautifulSoup
import requests

src = requests.get('https://help.imdb.com/article/contribution/other-submission-guides/country-codes/G99K4LFRMSC37DCN#')
soup = BeautifulSoup(src.text, 'html.parser')

codes = [x.text for x in soup.find('table').find_all('li')]
with open('./country_codes.tsv', 'w') as f:
    f.write('code\tcountry\n')
    for c in codes:
        code = c.partition(' ')[0].strip()
        country = c.partition(' ')[2].strip()
        f.write(f'{code}\t{country}\n')
