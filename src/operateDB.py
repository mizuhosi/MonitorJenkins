import os.path
import sqlite3


"""
 1つのcsvデータ格納フォルダに対してのDB操作を請け負う
"""


class operateDB:
    DB_NAME = "jenkinsdata.db"

    """
        引数で入力されたフォルダのDBと接続する
    """

    def __init__(self, dirPath):
        if not os.path.isdir(dirPath):
            raise NotADirectoryError("Input is not Directory!")

        self.dirPath = dirPath
        self.dbPath = os.path.join(self.dirPath, operateDB.DB_NAME)
        self.conn = sqlite3.connect(self.dbPath)

    def __del__(self):
        self.conn.close()
