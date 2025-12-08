import requests
from bs4 import BeautifulSoup
from datetime import datetime
from article import parse_article

months_dict = {
'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}

def convert_month(date_sub):
    submitted_date = date_sub.split(";")[0]
    day, month, year = submitted_date.split(" ")[1], submitted_date.split(" ")[2].strip(","), submitted_date.split(" ")[3]
    month = months_dict[month]
    return datetime(int(year), int(month), int(day))

base_url = "https://arxiv.org/search/?"
parameters = {"query": "+".join(["process", "mining"]),
              "searchtype": "all",
              "source": "header",
              "start": ""
              }
parameters_string = ""
for key, value in parameters.items():
    parameters_string += key + "=" + value + "&"

article_list = []
cat_url = base_url + parameters_string.strip("&")
for i in range(0, 100, 50):
    get_url = cat_url + str(i)
    text = requests.get(get_url)
    soup = BeautifulSoup(text.content, 'html.parser')

    preview_articles = [i for i in soup.find_all('li', class_="arxiv-result")]
    for art in preview_articles:
        title = art.find('p', class_="title is-5 mathjax").text.strip()
        date_sub = art.find('p', class_='is-size-7').text.strip()
        url_article = art.find('a').get("href").strip()
        date_sub = convert_month(date_sub)

        all_article = parse_article(url_article)
        result_dict = {"title": title[:50] + "...",
                       "data_sub": date_sub,
                       "url_article": url_article,
                       "text_article": all_article["text_article"][:50] + "...",
                       "authors": all_article["authors"]}

        article_list.append(result_dict)

for i in article_list:
    print(i)