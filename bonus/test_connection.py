import mysql.connector
from mysql.connector import errorcode


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


if __name__ == '__main__':
    main()
