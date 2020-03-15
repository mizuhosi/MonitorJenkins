import pytest

import os.path
import pandas as pd
import sqlite3

import shutil

from src.operateDB import operateDB


def test_construct_FolderExist(tmpdir):
    c = operateDB(tmpdir)
    assert c.dirPath == tmpdir


def test_construct_CreateDB(tmpdir):
    c = operateDB(tmpdir)
    assert os.path.isfile(c.dbPath)


def test_construct_InputError_01(tmpdir):
    # 存在するファイルパスが入力された場合の例外発生チェック
    testFilePath = os.path.join(tmpdir, "hoge.txt")
    with open(testFilePath, mode='w'):
        pass
    assert os.path.isfile(testFilePath)

    with pytest.raises(NotADirectoryError) as excinfo:
        c = operateDB(testFilePath)  # noqa: F841
    exceptionMsg = excinfo.value.args[0]
    assert exceptionMsg == "{0} is not Directory!".format(testFilePath)


def test_construct_InputError_02(tmpdir):
    # 存在しないパスが入力された場合の例外発生チェック
    testFilePath = os.path.join(tmpdir, "hoge.txt")
    assert not os.path.exists(testFilePath)

    with pytest.raises(NotADirectoryError) as excinfo:
        c = operateDB(testFilePath)  # noqa: F841
    exceptionMsg = excinfo.value.args[0]
    assert exceptionMsg == "{0} is not Directory!".format(testFilePath)


def test_destruct_deleteeDB(tmpdir):
    c = operateDB(tmpdir)
    assert os.path.isfile(c.dbPath)
    tmpDbPath = c.dbPath
    tmpConn = c.conn
    del c
    assert os.path.isfile(tmpDbPath)
    with pytest.raises(sqlite3.ProgrammingError):
        tmpConn.cursor()


def test_findFile_01(copyData):
    c = operateDB(copyData[0])
    c.findFiles('csv')
    assert c.fileList.sort() == copyData[1].sort()


def test_appendDB_01(copyData):
    c = operateDB(copyData[0])
    # 拡張子に"."在り
    c.appendDB(".csv")
    assert c.fileList.sort() == copyData[1].sort()


def test_appendDB_02(copyData):
    c = operateDB(copyData[0])
    # 拡張子に"."なし
    c.appendDB("csv")
    assert c.fileList.sort() == copyData[1].sort()


def test_appendDB_03(copyData):
    c = operateDB(copyData[0])
    # 未対応の拡張子
    c.appendDB("hoge")
    assert c.fileList == []


def test_appendDB_04(copyData, capsys):
    c = operateDB(copyData[0])
    # 未対応の拡張子
    c.appendDB("fuge")
    out, err = capsys.readouterr()
    assert out == "Not compatible for input extension: fuge\n"
    assert err == ""


def test_appendDB_05(copyData):
    # リネーム
    c = operateDB(copyData[0])
    c.appendDB("csv")
    assert not os.path.exists(os.path.join(copyData[0], "first.csv"))
    assert not os.path.exists(os.path.join(copyData[0], "second.csv"))
    assert not os.path.exists(os.path.join(copyData[0], "third.csv"))
    assert os.path.exists(os.path.join(copyData[0], "_first.csv"))
    assert os.path.exists(os.path.join(copyData[0], "_second.csv"))
    assert os.path.exists(os.path.join(copyData[0], "_third.csv"))


def test_appendDB_CSV_01(copyData):
    c = operateDB(copyData[0])
    c.appendDB_CSV(copyData[1])
    df = pd.read_sql('SELECT * from MONITOR', c.conn)
    tmp = df.values.tolist()
    testList = [['2019/3/4', 8.6, 14.6, 10.5, 10],      # first.csv
                ['2019/3/5', 7.7, 17, 11.5, 0],         # second.csv
                ['2019/3/6', 7.6, 16.1, 11.6, 8],       # second.csv
                ['2019/3/8', 4, 12, 7.7, 0],            # third.csv
                ['2019/3/9', -0.1, 15.6, 8.3, 0],       # third.csv
                ['2019/3/10', 5.7, 11.9, 9.6, 28],      # third.csv
                ['2019/3/11', 9.4, 15.1, 11.3, 11.5]]   # third.csv
    assert tmp.sort() == testList.sort()


@pytest.fixture
def copyData(tmpdir):
    """tmpdirにテストデータをコピーする

    Returns:
        List:
            0: データをコピーしたフォルダ
            1: コピーしたデータファイルパスのリスト
    """
    dataPath = os.path.join(os.path.dirname(__file__), "Data")
    tmpDataPath = os.path.join(tmpdir, "Data")
    shutil.copytree(dataPath, tmpDataPath)

    # copyが成功しているか確認
    firstPath = os.path.join(tmpDataPath, "first.csv")
    secondPath = os.path.join(tmpDataPath, "second.csv")
    thirdPath = os.path.join(tmpDataPath, "third.csv")
    assert os.path.isfile(firstPath)
    assert os.path.isfile(secondPath)
    assert os.path.isfile(thirdPath)

    return [tmpDataPath, [firstPath, secondPath, thirdPath]]
