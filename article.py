import requests
from bs4 import BeautifulSoup
#
# prefix_art_url = "https://arxiv.org/abs/"

def parse_article(url):
    text = requests.get(url)
    soup = BeautifulSoup(text.content, 'html.parser')

    authors = soup.find('div', class_='authors')
    authors_dict = {i.text: i.get('href') for i in authors.find_all('a')}
    text_article = soup.find('blockquote').text.strip()

    return {"authors": authors_dict, "text_article": text_article}
#
# dict_ = parse_article("2512.03906")
# for key, value in dict_.items():
#     print(key, " ", value)

