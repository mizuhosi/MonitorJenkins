import os.path

import pytest
import sqlite3

from src.operateDB import operateDB


def test_construct_FolderExist(tmpdir):
    c = operateDB(tmpdir)
    assert c.dirPath == tmpdir


def test_construct_CreateDB(tmpdir):
    c = operateDB(tmpdir)
    assert os.path.isfile(c.dbPath)


def test_construct_InputFile(tmpdir):
    with pytest.raises(NotADirectoryError) as excinfo:
        c = operateDB(os.path.join(tmpdir, "hoge.txt"))  # noqa: F841
    exceptionMsg = excinfo.value.args[0]
    assert exceptionMsg == "Input is not Directory!"


def test_destruct_deleteeDB(tmpdir):
    c = operateDB(tmpdir)
    assert os.path.isfile(c.dbPath)
    tmpDbPath = c.dbPath
    tmpConn = c.conn
    del c
    assert os.path.isfile(tmpDbPath)
    with pytest.raises(sqlite3.ProgrammingError):
        tmpConn.cursor()
