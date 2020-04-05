"""operateDB.py

    * データ格納フォルダに対してのDB操作を請け負う
    * DB名は固定　$DB_NAME = jenkinsdata.db
Todo:
    TODOリストを記載する

"""
import glob
import os
import pandas as pd
import sqlite3


class operateDB:
    """ DBを操作する

    Attributes:
        dirPath(str)    : 操作対象のディレクトパス
        dbPath(str)     : DBパス
        extension(str)  : 対象ファイルの拡張子
        fileList(List(str)) : 対象ファイルのパスのリスト
        conn(sqlite3.Connection) : DBのコネクタ
    """
    DB_NAME = "jenkinsdata.db"
    DB_TBL = "MONITOR"

    def __init__(self, dirPath):
        """データ格納フォルダ内のDBと接続する
        Args:
            dirPath(str): データ格納フォルダへのパス

        Raise:
            NotADirectoryError: dirPathがフォルダへのパスではない場合発生する

        """
        if not os.path.isdir(dirPath):
            raise NotADirectoryError(dirPath + " is not Directory!")

        self.dirPath = dirPath
        self.dbPath = os.path.join(self.dirPath, operateDB.DB_NAME)
        self.conn = sqlite3.connect(self.dbPath)

    def __del__(self):
        if hasattr(self, "conn"):
            self.conn.close()

    def findFiles(self, extension):
        """データファイル一覧のリストを作成する
                拡張子が入力と一致し、ファイル名の1文字目が"_"でない場合データファイルと認識する

        Args:
            extension(str): データの拡張子　ex:csv
        """
        fileList = glob.glob("{0}\\*.{1}".format(self.dirPath, extension))
        self.fileList = [f for f in fileList if os.path.basename(f)[0] != '_']

    def appendDB(self, extension):
        """ファイルを読み込んで内容をDBに追記する
        Args:
            extension(Str): データの拡張子  ex:.csv
            fileList(List): ファイルリスト  ex:[aaa.csv, bbb.csv, ...]

        """
        if extension[0] == '.':
            extension = extension[1:]
        self.findFiles(extension)

        if self.fileList == []:
            return

        if extension == 'csv':
            self.appendDB_CSV(self.fileList)
        else:
            print("Not compatible for input extension: {0}".format(extension))

        for f in self.fileList:
            name = os.path.join(os.path.dirname(f), '_' + os.path.basename(f))
            os.rename(f, name)

    def appendDB_CSV(self, fileList):
        """CSVファイルを読み込んで内容をDBに追記する

        Args:
            fileList(List(Str)): 読み込むCSVのパスのリスト
        """
        for f in fileList:
            df = pd.read_csv(f)
            df.to_sql(operateDB.DB_TBL, self.conn, if_exists='append', index=False)

        self.conn.commit()

    def output_excel(self):
        cur = self.conn.cursor()
        sqlOperate = "SELECT * FROM {0}".format(operateDB.DB_TBL)
        df = pd.readsql(sqlOperate, self.conn)
        df.to_excel(os.path.basename(self.dirPath) + ".xlsx")
        cur.close()
