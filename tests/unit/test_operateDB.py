import os.path

import pytest
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
    # 拡張子に"."なし
    c = operateDB(copyData)
    c.findFiles('.csv')
    firstPath = os.path.join(copyData, "first.csv")
    secondPath = os.path.join(copyData, "second.csv")
    thirdPath = os.path.join(copyData, "thirid.csv")
    assert c.fileList.sort() == [firstPath, secondPath, thirdPath].sort()


def test_findFile_02(copyData):
    # 拡張子に"."あり
    c = operateDB(copyData)
    c.findFiles('.csv')
    firstPath = os.path.join(copyData, "first.csv")
    secondPath = os.path.join(copyData, "second.csv")
    thirdPath = os.path.join(copyData, "thirid.csv")
    assert c.fileList.sort() == [firstPath, secondPath, thirdPath].sort()


@pytest.fixture
def copyData(tmpdir):
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

    return tmpDataPath
