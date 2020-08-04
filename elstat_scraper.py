import itertools
import os
import pathlib

import mysql.connector
import matplotlib.pyplot
import numpy
import pandas
import requests
import xlrd
import codecs
from bs4 import BeautifulSoup
from mysql.connector import errorcode
from transliterate import slugify


# function that converts xlrd object into pandas dataframe
def xlrd2dataframe(xl_file, sheet_names):
    for sheet in sheet_names:
        # minimize memory usage with the yield command
        yield pandas.read_excel(xl_file, sheet_name=sheet, engine='xlrd',
                                header=None, index_col=False).assign(source=sheet)
        xl_file.unload_sheet(sheet)


# function to preprocess scraped data
def preprocess_data():
    # scan the directory with the scraped data
    local_file_database = [f for f in os.listdir(str(pathlib.Path().absolute()) + '\\scraper_data')
                           if os.path.isfile(os.path.join(str(str(pathlib.Path().absolute()) + '\\scraper_data'), f))]
    # filenames are similar, so we can sort them and manage them efficiently
    local_file_database.sort()
    # create the dataframe to store the preprocessed data
    data = pandas.DataFrame()
    # initialize keys based on xlsx sheet names
    months = ['ΙΑΝ', 'ΦΕΒ', 'ΜΑΡ', 'ΑΠΡ', 'ΜΑΙ', 'ΙΟΥΝ', 'ΙΟΥΛ', 'ΑΥΓ', 'ΣΕΠΤ', 'ΟΚΤ', 'ΝΟΕΜΒ', 'ΔΕΚΕΜ']
    # preprocess data by year
    for idx in range(0, len(local_file_database), 3):
        # initialize a dataframe holder for temporary data
        result = pandas.DataFrame()
        # loop for every month
        for month in months:
            # hold the year of the data
            year = local_file_database[idx][0:4]
            # import the data grouped by country
            arr = pandas.read_excel(pathlib.Path(str(pathlib.Path().absolute()) +
                                                 '\\scraper_data\\' + local_file_database[idx + 1]), header=None,
                                    index_col=False).dropna().drop(columns=[0])
            # arrivals by country - preprocessing
            arr = arr[arr[8] == month].reset_index(drop=True)
            # delete every dot from first column
            arr[1] = arr[1].map(lambda x: x.rstrip('.'))
            # drop duplicates
            arr = arr[arr[1].to_numpy(dtype=numpy.int64) > arr.index]
            # round floating data to nearest integer value
            arr[4] = numpy.round(arr[4].astype(numpy.float64)).astype(numpy.int64)
            # convert column 7 data from string to floating point values
            arr[7] = arr[7].astype(numpy.float16)
            # drop unused columns and rename the rest for readability
            arr = arr.drop(columns=[1, 3, 5, 6, 8]).rename(columns={2: "country", 4: "tourists", 7: "percentage"})
            # arrivals by means of transportation - preprocessing
            tr = pandas.read_excel(pathlib.Path(str(pathlib.Path().absolute()) +
                                                '\\scraper_data\\' + local_file_database[idx]), header=None,
                                   index_col=False).dropna().drop(columns=[0])
            # drop unused column
            tr = tr[tr[8] == month].reset_index(drop=True)
            # delete every dot from first column
            tr[1] = tr[1].map(lambda x: x.rstrip('.'))
            # drop duplicates
            tr = tr[tr[1].to_numpy(dtype=numpy.int64) > tr.index]
            # drop unused columns and rename the rest for readability
            tr = tr.drop(columns=[1, 7, 8]).rename(
                columns={2: "country", 3: "airplane", 4: "train", 5: "ship", 6: "car"})
            # convert floating point values into integer values
            tr["airplane"] = numpy.round(tr["airplane"].astype(numpy.float64)).astype(numpy.int64)
            tr["train"] = numpy.round(tr["train"].astype(numpy.float64)).astype(numpy.int64)
            tr["ship"] = numpy.round(tr["ship"].astype(numpy.float64)).astype(numpy.int64)
            tr["car"] = numpy.round(tr["car"].astype(numpy.float64)).astype(numpy.int64)
            # merge the 2 previous dataframes into a single one
            month_df = pandas.merge(arr, tr, on='country')
            # initialize a new column which is equal to the month of the data
            month_df['month'] = month
            # initialize month column
            result = pandas.concat([result, month_df])
        # initialize year column
        result['year'] = numpy.int16(year)
        # concatenate the year data with the whole dataset
        data = pandas.concat([data, result])
    # reset index of dataframe
    data = data.reset_index(drop=True)
    # create directory for preprocessed data
    if not os.path.exists(str(pathlib.Path().absolute()) + '\\preprocessed_data'):
        os.makedirs(str(pathlib.Path().absolute()) + '\\preprocessed_data')
    # initialize filename variable
    filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    # save file if not exists
    if not filename.is_file():
        data.to_csv(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv', index=False, encoding='utf-8-sig')


# convert files int utf8 [code found from:
# https://stackoverflow.com/questions/8898294/convert-utf-8-with-bom-to-utf-8-with-no-bom-in-python]
def encode_file():
    BUFSIZE = 4096
    BOMLEN = len(codecs.BOM_UTF8)

    path = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    with open(path, "r+b") as fp:
        chunk = fp.read(BUFSIZE)
        if chunk.startswith(codecs.BOM_UTF8):
            i = 0
            chunk = chunk[BOMLEN:]
            while chunk:
                fp.seek(i)
                fp.write(chunk)
                i += len(chunk)
                fp.seek(BOMLEN, os.SEEK_CUR)
                chunk = fp.read(BUFSIZE)
            fp.seek(-BOMLEN, os.SEEK_CUR)
            fp.truncate()


def graph_1():
    # import data
    filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    data = pandas.read_csv(filename)
    x = numpy.zeros(5, dtype=numpy.int64)
    y = numpy.zeros(5, dtype=numpy.int64)
    for idx, year in enumerate(range(2011, 2016)):
        x[idx] = int(year)
        y[idx] = int(sum(data[data.year == year].tourists))
    matplotlib.pyplot.figure(figsize=(10, 10))
    matplotlib.pyplot.plot(x, y)
    matplotlib.pyplot.xticks(x, numpy.arange(2011, 2016, dtype=numpy.int64))
    matplotlib.pyplot.xlabel('Year')
    matplotlib.pyplot.ylabel('Tourists')
    matplotlib.pyplot.savefig('graph_1.png')


def graph_2():
    # import data
    filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    data = pandas.read_csv(filename)
    x = numpy.zeros(len(data.country.unique()), dtype=numpy.int64)
    y = numpy.zeros(len(data.country.unique()), dtype=numpy.int64)
    for idx, country in enumerate(data.country.unique()):
        x[idx] = idx
        y[idx] = sum(data[data.country == country].tourists)
    labels = data.country.unique()
    matplotlib.pyplot.figure(figsize=(25, 15))
    matplotlib.pyplot.plot(x, y)
    matplotlib.pyplot.xticks(x, labels, rotation=70)
    matplotlib.pyplot.margins(0.01)
    matplotlib.pyplot.subplots_adjust(bottom=0.15)
    matplotlib.pyplot.xlabel('Country')
    matplotlib.pyplot.ylabel('Tourists')
    matplotlib.pyplot.savefig('graph_2.png')


def graph_3():
    # import data
    filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    data = pandas.read_csv(filename)
    x = numpy.arange(len(data.columns[3:7]), dtype=numpy.int64)
    y = numpy.zeros(4, dtype=numpy.int64)
    for idx, tr_way in enumerate(data.columns[3:7]):
        y[idx] = sum(data[tr_way])
    labels = data.columns[3:7]
    matplotlib.pyplot.figure(figsize=(15, 10))
    matplotlib.pyplot.plot(x, y)
    matplotlib.pyplot.xticks(x, labels, rotation=45)
    matplotlib.pyplot.margins(0.01)
    matplotlib.pyplot.subplots_adjust(bottom=0.2)
    matplotlib.pyplot.xlabel('Means of Transport')
    matplotlib.pyplot.ylabel('Tourists')
    matplotlib.pyplot.savefig('graph_3.png')


def graph_4():
    # import data
    filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    data = pandas.read_csv(filename)
    quarters = numpy.split(data.month.unique(), 4)
    x = numpy.arange(len(quarters), dtype=numpy.int64)
    y = numpy.zeros(4, dtype=numpy.int64)
    labels = list()
    for idx, quarter in enumerate(quarters):
        y[idx] = sum(data[data.month.isin(list(quarter))].tourists)
        labels.append(' '.join(list(quarter)))
    matplotlib.pyplot.figure(figsize=(15, 10))
    matplotlib.pyplot.plot(x, y)
    matplotlib.pyplot.xticks(x, labels, rotation=45)
    matplotlib.pyplot.margins(0.01)
    matplotlib.pyplot.subplots_adjust(bottom=0.3)
    matplotlib.pyplot.xlabel('Quarter')
    matplotlib.pyplot.ylabel('Tourists')
    matplotlib.pyplot.savefig('graph_4.png')


# function that exports requested graphs
def export_graph_data():
    graph_1()
    graph_2()
    graph_3()
    graph_4()


# function that inserts dataframe row into mysql table
def insert2mysql(id, country, tourists, percentage, airplane, train, ship, car, month, year):
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='elstat',
                                             user='root',
                                             password='CeiD/16.')
        cursor = connection.cursor()
        query = """INSERT INTO records (id, country, tourists, percentage, airplane, train, ship, car, month, year) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        record = (id, country, tourists, percentage, airplane, train, ship, car, month, year)
        cursor.execute(query, record)
        connection.commit()

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# function that connects with mysql and performs the requested uploading
def upload2mysql(evaluate):
    # create database
    try:
        connection = mysql.connector.connect(user='root', password='CeiD/16.',
                                             host='127.0.0.1')
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS elstat CHARACTER SET utf8mb4;")
        connection.commit()
        cursor.close()
        connection.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    # create table
    try:
        connection = mysql.connector.connect(user='root', password='CeiD/16.',
                                             host='127.0.0.1',
                                             db='elstat')
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS records;")
        cursor.execute("CREATE TABLE records ( "
                       "id INT NOT NULL, "
                       "country VARCHAR(30) NOT NULL, "
                       "tourists INT NOT NULL, "
                       "percentage DOUBLE NOT NULL, "
                       "airplane INT NOT NULL, "
                       "train INT NOT NULL, "
                       "ship INT NOT NULL, "
                       "car INT NOT NULL, "
                       "month VARCHAR(7) NOT NULL, "
                       "year VARCHAR(5) NOT NULL, "
                       "PRIMARY KEY (id)"
                       ")ENGINE=INNODB;")

        connection.commit()
        cursor.close()
        connection.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)

    # insert data
    filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\preprocessed_data\\data.csv')
    data = pandas.read_csv(filename)
    # Connect to the database
    connection = mysql.connector.connect(host='localhost',
                                         user='root',
                                         password='CeiD/16.',
                                         db='elstat')

    # create cursor
    cursor = connection.cursor()

    # perform insertions into mysql table
    for id in range(data.shape[0]):
        country, tourists, percentage, airplane, train, ship, car, month, year = data.iloc[id]
        insert2mysql(int(id), country, int(tourists), float(percentage), int(airplane), int(train), int(ship),
                     int(car), month, int(year))

    # evaluate the correctness of the code above by selecting first 20 rows of the created mysql table
    if evaluate:
        sql = "SELECT * FROM records LIMIT 20"
        cursor.execute(sql)

        # Fetch all the records
        result = cursor.fetchall()
        for rec in result:
            print(rec)

    connection.commit()
    cursor.close()
    connection.close()


# function that does the scraping
def scrap_elstat_data():
    # cartesian product of all combinations in given lists
    arrivals_itr_product = itertools.product(['https://www.statistics.gr/el/statistics/-/publication/STO04/'],
                                             numpy.arange(2011, 2016, dtype=numpy.int64).tolist(),
                                             ['-Q4'])
    # generate the urls
    links = list()
    # generate the filenames
    ids = list()

    # concatenate elements in tuple
    for tpl in arrivals_itr_product:
        link = "".join(list(map("".join, list(str(element) for element in tpl))))
        links.append(link)
        ids.append(str(tpl[1]))
    # scrap elstat
    for link in links:
        # request the html code
        data = requests.get(link).text
        # parse the html code with BeautifulSoup
        soup = BeautifulSoup(data, 'lxml')
        # find the division that contains the xlsx links
        table = soup.find('div', {"id": "_documents_WAR_publicationsportlet_INSTANCE_VBZOni0vs5VJ_"})
        # trace the table with the files
        rows = table.find_all('tr', recursive=True)
        # download every file that is contained in the table
        for row in rows:
            # trace the link
            r = requests.get(row.find('a').get('href'))
            # load the requested file link into an xlrd workbook
            workbook_xlrd = xlrd.open_workbook(file_contents=r.content)
            # convert xlrd workbook into pandas DataFrame
            workbook_df = pandas.concat(xlrd2dataframe(workbook_xlrd, workbook_xlrd.sheet_names()), ignore_index=True)
            # create directory to store scraped data
            if not os.path.exists(str(pathlib.Path().absolute()) + '\\scraper_data'):
                os.makedirs(str(pathlib.Path().absolute()) + '\\scraper_data')
            # initialize filename for the newly scraped xlsx file
            filename = pathlib.Path(str(pathlib.Path().absolute()) + '\\scraper_data\\' + ids[0] + '-' +
                                    slugify(''.join(c for c in row.text.strip() if not c.isdigit())) + '.xlsx')
            # save dataframe if not exists
            if not filename.is_file():
                workbook_df.to_excel(filename)

        del ids[0]


# driver function
def main():
    scrap_elstat_data()
    preprocess_data()
    encode_file()
    upload2mysql(evaluate=True)
    export_graph_data()


# main thread
if __name__ == '__main__':
    main()
