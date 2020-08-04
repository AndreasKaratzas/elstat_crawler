from bs4 import BeautifulSoup
import requests


def main():
    data = requests.get("https://www.statistics.gr/el/statistics/-/publication/STO09/2015").text
    soup = BeautifulSoup(data, 'lxml')
    table = soup.find('div', {"id": "_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_"})
    rows = table.find_all('tr', recursive=True)
    for row in rows:
        print(row.text)
        print(row.find('a').get('href'))


if __name__ == '__main__':
    main()
