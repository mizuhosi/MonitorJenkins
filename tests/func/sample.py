import os.path
import sqlite3

from src.operateDB import operateDB


def sampleExecution():
    c = operateDB(os.path.join(os.path.dirname(__file__), 'Data'))
    c.appendDB('csv')

    cur = c.conn.cursor()
    cur.execute('select * from MONITOR')

    for row in cur:
        print(row)


if __name__ == "__main__":
    sampleExecution()
