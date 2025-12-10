import requests
from bs4 import BeautifulSoup
from datetime import datetime
from article import parse_article
import os
import random
import secrets
import re
import json

class ARXIVParser:
    def __init__(self):
        self.months_dict = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }

        self.base_url = "https://arxiv.org/search/?"
        self.parameters = {"query": "+".join(["process", "mining"]),
                      "searchtype": "all",
                      "source": "header",
                      "start": ""
                      }
        self.article_list = []
        self.parameters_string = ""
        self.__pre_convert_url()
        self.cat_url = self.base_url + self.parameters_string.strip("&")
    def __pre_convert_url(self):
        for key, value in self.parameters.items():
            self.parameters_string += key + "=" + value + "&"
    def __convert_month(self, date_sub):
        submitted_date = date_sub.split(";")[0]
        day, month, year = submitted_date.split(" ")[1], submitted_date.split(" ")[2].strip(","), submitted_date.split(" ")[3]
        month = self.months_dict[month]
        return f"{year}-{month}-{day}"


    def __parsing_page_articles(self):
        for i in range(0, 100, 50):
            get_url = self.cat_url + str(i)
            text = requests.get(get_url)
            soup = BeautifulSoup(text.content, 'html.parser')

            preview_articles = [i for i in soup.find_all('li', class_="arxiv-result")]
            for art in preview_articles:
                title = art.find('p', class_="title is-5 mathjax").text.strip()
                date_sub = art.find('p', class_='is-size-7').text.strip()
                url_article = art.find('a').get("href").strip()
                date_sub = self.__convert_month(date_sub)

                all_article = parse_article(url_article)
                result_dict = {"title": title,
                               "data_sub": date_sub,
                               "url_article": url_article,
                               "text_article": all_article["text_article"],
                               "authors": all_article["authors"]}
                self.article_list.append(result_dict)
            break
        return self.article_list

    def __make_md(self):
        os.makedirs("result_docs", exist_ok=True)
        for i in self.article_list:
            # random_hex_string = secrets.token_hex(5)
            text_article = i["text_article"]
            title = i["title"]
            title = re.sub(r"[\s:]+", "_", title) # Чистим статьи от нежелательного " : "
            # file_name = f"{title}~{random_hex_string}.md"
            file_name = f"{title}.md"
            i["file_name"] = file_name
            with open(os.path.join("result_docs", file_name), mode='w', encoding='utf-8') as file:
                file.write(text_article)

    def __make_json(self):
        meta_list = self.article_list.copy()
        for i in meta_list:
            i.pop("text_article", None)
        with open("articles.json", "w", encoding="utf-8") as f:
            json.dump(meta_list, f, ensure_ascii=False, indent=2)

    def run(self):
        self.__parsing_page_articles()
        self.__make_md()
        self.__make_json()

parser = ARXIVParser()
parser.run()


