import itertools
import os
import pathlib

import mysql.connector
import numpy
import pandas
import requests
import xlrd
from bs4 import BeautifulSoup
from mysql.connector import errorcode


def xlrd2dataframe(xl_file, sheet_names):
    for sheet in sheet_names:
        yield pandas.read_excel(xl_file, sheet_name=sheet, engine='xlrd').assign(source=sheet)
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

    arrivals_itr_product = itertools.product(['https://www.statistics.gr/el/statistics/-/publication/'],
                                             ['STO04'],
                                             ['/'],
                                             numpy.arange(2011, 2016, dtype=numpy.int64).tolist(),
                                             ['-Q'],
                                             numpy.arange(1, 5, dtype=numpy.int64).tolist())

    rest_itr_product = itertools.product(['https://www.statistics.gr/el/statistics/-/publication/'],
                                         ['STO09', 'STO12', 'STO15'],
                                         ['/'],
                                         numpy.arange(2011, 2016, dtype=numpy.int64).tolist())
    links = list()

    ids = list()

    for tpl in arrivals_itr_product:
        link = "".join(list(map("".join, list(str(element) for element in tpl))))
        links.append(link)
        ids.append("-".join([tpl[1], str(tpl[3]), str(tpl[5])]))

    for tpl in rest_itr_product:
        link = "".join(list(map("".join, list(str(element) for element in tpl))))
        links.append(link)
        ids.append("-".join([tpl[1], str(tpl[3])]))

    for link in links:
        data = requests.get(link).text
        soup = BeautifulSoup(data, 'lxml')
        table = soup.find('div', {"id": "_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_"})
        rows = table.find_all('tr', recursive=True)
        for idx, row in enumerate(rows):
            r = requests.get(row.find('a').get('href'))
            workbook_xlrd = xlrd.open_workbook(file_contents=r.content)
            workbook_df = pandas.concat(xlrd2dataframe(workbook_xlrd, workbook_xlrd.sheet_names()), ignore_index=True)

            if not os.path.exists(str(pathlib.Path().absolute()) + '\\data'):
                os.makedirs(str(pathlib.Path().absolute()) + '\\data')

            # filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\data\\' + ids[0] + '-' +
            #                         slugify(''.join(c for c in row.text.strip() if not c.isdigit())) + '.xlsx')

            filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\data\\' + ids[0] + '-' + str(idx) + '.xlsx')

            if not filename.is_file():
                workbook_df.to_excel(filename)

        del ids[0]


if __name__ == '__main__':
    main()
