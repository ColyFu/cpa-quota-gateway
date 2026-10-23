"""Merge stopped analytics/quota stores, retaining every existing table and row.

Usage: python3 migrate_shared_store.py ANALYTICS_DB QUOTA_DB OUTPUT_DB
Stop both writers before running this command. Sources are opened read-only.
The output must be new; source files are never replaced or deleted here.
"""
import hashlib
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def digest(conn, table):
    sha = hashlib.sha256()
    count = 0
    for row in conn.execute('SELECT * FROM ' + quoted(table) + ' ORDER BY rowid'):
        sha.update(repr(row).encode())
        sha.update(b'\n')
        count += 1
    return count, sha.digest()


def merge(analytics, quota, output):
    output = Path(output)
    if output.exists():
        raise ValueError('output already exists')
    output.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    with closing(sqlite3.connect(f'file:{Path(analytics).resolve()}?mode=ro', uri=True)) as source, \
         closing(sqlite3.connect(f'file:{Path(quota).resolve()}?mode=ro', uri=True)) as ledger, \
         closing(sqlite3.connect(output)) as target:
        source.backup(target)
        target.execute('ATTACH DATABASE ? AS quota_source', (f'file:{Path(quota).resolve()}?mode=ro',))
        tables = ledger.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
        for name, definition in tables:
            if target.execute('SELECT 1 FROM main.sqlite_master WHERE name=?', (name,)).fetchone():
                raise ValueError('table name collision: ' + name)
            target.execute(definition)
            target.execute(f'INSERT INTO main.{quoted(name)} SELECT * FROM quota_source.{quoted(name)}')
        for (definition,) in ledger.execute("SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"):
            target.execute(definition)
        target.commit()
        target.execute('DETACH DATABASE quota_source')
        for original in [source, ledger]:
            for (name,) in original.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
                if digest(original, name) != digest(target, name):
                    raise RuntimeError('verification failed: ' + name)
        if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise RuntimeError('database integrity check failed')
        target.execute('VACUUM')
        target.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    return output.stat().st_size


if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    print('Verified shared database bytes:', merge(*sys.argv[1:]))
