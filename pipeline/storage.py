"""Local labelled mart and atomic model registry shared between services."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3

ROOT = Path(os.getenv('RUNTIME_DIR', '/opt/project/runtime'))


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def active():
    path = ROOT / 'models/active.json'
    return json.loads(path.read_text()) if path.exists() else None


@contextmanager
def lock():
    ROOT.mkdir(parents=True, exist_ok=True)
    with (ROOT / 'pipeline.lock').open('a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def connect():
    ROOT.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(ROOT / 'mart.sqlite', timeout=60)
    connection.execute('PRAGMA journal_mode=WAL')
    connection.execute('CREATE TABLE IF NOT EXISTS transactions (id TEXT PRIMARY KEY, month TEXT NOT NULL, payload TEXT NOT NULL)')
    connection.execute('CREATE INDEX IF NOT EXISTS transactions_month ON transactions(month)')
    return connection


def rows(connection, months):
    values = connection.execute('SELECT payload FROM transactions WHERE month IN (' + ','.join('?' for _ in months) + ') ORDER BY id', months)
    return [json.loads(value[0]) for value in values]


def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()
