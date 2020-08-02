import mysql.connector
import wget
import xlrd
from mysql.connector import errorcode
from bs4 import BeautifulSoup
import requests


def main():
    try:
        cnx = mysql.connector.connect(user='root', password='CeiD/16.',
                                      host='127.0.0.1',
                                      database='crawler')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cnx.close()

    data = requests.get("https://www.statistics.gr/el/statistics/-/publication/STO04/2016-Q2").text
    soup = BeautifulSoup(data, 'html.parser')

    for link in soup.find_all("a", recursive=True):
        if link.text.strip() == u'01. Αφίξεις μη κατοίκων από το εξωτερικό ανά χώρα προέλευσης':
            print("Inner Text: {}".format(link.text.strip()))
            print("href: {}".format(link.get("href")))
            r = requests.get(link.get("href"))
            workbook = xlrd.open_workbook(file_contents=r.content)  # open workbook
            worksheet = workbook.sheet_by_index(0)                  # get first sheet
            first_row = worksheet.row(0)                            # you can iterate over rows of a worksheet as well
            print(first_row)                                        # list of cells


if __name__ == '__main__':
    main()

