"""
 1つのcsvデータ格納フォルダに対してのDB操作を請け負う
"""
import glob
import os.path
import sqlite3


class operateDB:
    """
        引数で入力されたフォルダのDBと接続する
        DB名は固定($DB_NAME)とする。データ格納フォルダ単位で処理するため問題ないはず
    """

    def __init__(self, dirPath):
        if not os.path.isdir(dirPath):
            raise NotADirectoryError(dirPath + " is not Directory!")

        self.dirPath = dirPath

        DB_NAME = "jenkinsdata.db"
        self.dbPath = os.path.join(self.dirPath, DB_NAME)
        self.conn = sqlite3.connect(self.dbPath)

    def __del__(self):
        if hasattr(self, "conn"):
            self.conn.close()

    def findFiles(self, extension):
        if extension[0] == '.':
            extension = extension[1:]
        self.fileList = glob.glob("{0}\\*.{1}".format(self.dirPath, extension))
        self.fileList = [f for f in self.fileList if os.path.basename(f)[0] != '_']

    # def readFile(self):
    #     pass

    # def readCSV(self, parameter_list):
    #     pass
