import pandas
import xlrd
import numpy
import requests
import itertools
import mysql.connector
from bs4 import BeautifulSoup
from mysql.connector import errorcode


def df_gen(xl_file, sheet_names):
    for sheet in sheet_names:
        yield pandas.read_excel(xl_file, sheet_name=sheet, engine='xlrd').assign(source=sheet)
        # tell xlrd to let the sheet leave memory
        xl_file.unload_sheet(sheet)


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

    combos = itertools.product(numpy.arange(2011, 2016, dtype=numpy.int64).tolist(),
                               ['-Q'],
                               numpy.arange(1, 5, dtype=numpy.int64).tolist())

    data = requests.get("https://www.statistics.gr/el/statistics/-/publication/STO04/2016-Q2").text
    soup = BeautifulSoup(data, 'html.parser')

    for link in soup.find_all("a", recursive=True):
        if link.text.strip() == u'01. Αφίξεις μη κατοίκων από το εξωτερικό ανά χώρα προέλευσης':
            print("Inner Text: {}".format(link.text.strip()))
            print("href: {}".format(link.get("href")))
            r = requests.get(link.get("href"))

            workbook = xlrd.open_workbook(file_contents=r.content)  # open workbook

            # worksheet = workbook.sheet_by_index(0)                  # get first sheet
            df = pandas.concat(df_gen(workbook, workbook.sheet_names()), ignore_index=True)
            print(df.head())
            # first_row = worksheet.row(0)                            # you can iterate over rows of a worksheet as well
            # print(first_row)                                        # list of cells


if __name__ == '__main__':
    main()
