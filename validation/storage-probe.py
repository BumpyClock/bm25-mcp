#!/usr/bin/env python3
"""Synthetic transaction and recovery probe, not an MCP implementation."""
import json
import os
from pathlib import Path
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time

COUNT = 73101


def connect(path):
    db = sqlite3.connect(path, isolation_level=None, timeout=10)
    db.execute('PRAGMA synchronous=FULL')
    return db


def crash(path, committed):
    db = connect(path)
    db.execute('BEGIN IMMEDIATE')
    db.execute('UPDATE chunks SET length=length+1 WHERE id=0')
    db.execute('UPDATE state SET generation=generation+1,total=total+1')
    if committed:
        db.execute('COMMIT')
    os._exit(23)


def snapshot(db):
    db.execute('BEGIN')
    state = db.execute('SELECT generation,total FROM state').fetchone()
    actual = db.execute('SELECT sum(length) FROM chunks').fetchone()[0]
    db.execute('COMMIT')
    assert state[1] == actual, 'mixed generation or incorrect statistics'
    return state


def main():
    report = {'sqlite_version': sqlite3.sqlite_version, 'platform': sys.platform,
              'scope': 'Synthetic chunk payload persistence and consistent SQL snapshots; no BM25 search or in-memory posting publication.',
              'chunks': COUNT, 'payload_bytes_per_chunk': 1800,
              'settings': {'journal_mode': 'WAL', 'synchronous': 'FULL', 'writer_connections': 1, 'reader_threads': 4},
              'batches': [], 'limits': ['No power-loss test', 'No Windows test', 'No real source text',
              'SQL snapshot checks are not search latency measurements',
              'In-memory posting publication cost remains unmeasured',
              'SQLite 3.51.0 is the local probe runtime; production must use a WAL-reset-fixed release. Only one connection writes/checkpoints during this probe.']}
    with tempfile.TemporaryDirectory(prefix='bm25-storage-') as directory:
        path = str(Path(directory) / 'probe.sqlite')
        db = connect(path)
        assert db.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        db.executescript('CREATE TABLE chunks(id INTEGER PRIMARY KEY,length INTEGER,payload TEXT); CREATE INDEX chunk_lengths ON chunks(length); CREATE TABLE state(generation INTEGER,total INTEGER);')
        db.execute('BEGIN')
        db.executemany('INSERT INTO chunks VALUES(?,?,?)', ((i,200,'x'*1800) for i in range(COUNT)))
        db.execute('INSERT INTO state VALUES(0,?)',(COUNT*200,))
        db.execute('COMMIT')
        stop = threading.Event()
        barrier = threading.Barrier(5)
        reads, errors = [], []

        def reader():
            connection = connect(path)
            connection.execute('PRAGMA query_only=ON')
            completed = 0
            try:
                barrier.wait()
                while not stop.is_set():
                    snapshot(connection)
                    completed += 1
            except Exception as error:
                errors.append(type(error).__name__ + ': ' + str(error))
            finally:
                reads.append(completed)
                connection.close()

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for thread in threads:
            thread.start()
        barrier.wait()
        try:
            for count in (1,23,7219):
                samples = []
                for repeat in range(5):
                    start = time.perf_counter()
                    db.execute('BEGIN IMMEDIATE')
                    db.executemany('UPDATE chunks SET length=length+1,payload=? WHERE id=?',
                                   ((('x' if repeat%2 else 'y')*1800,i) for i in range(count)))
                    db.execute('UPDATE state SET generation=generation+1,total=total+?',(count,))
                    db.execute('COMMIT')
                    samples.append((time.perf_counter()-start)*1000)
                report['batches'].append({'changed_chunks':count,'samples_ms':samples,'median_ms':statistics.median(samples),'max_ms':max(samples)})
        finally:
            stop.set()
            for thread in threads:
                thread.join()
        assert not errors, errors
        assert all(reads), 'every reader must complete a snapshot'
        report['consistent_reader_snapshots'] = sum(reads)
        report['reader_counts'] = reads
        report['reader_errors'] = errors
        before = snapshot(db)
        for committed in (False,True):
            child = subprocess.run([sys.executable,__file__,'--crash',path,str(int(committed))])
            assert child.returncode == 23
            connection = connect(path)
            after = snapshot(connection)
            expected = (before[0]+int(committed),before[1]+int(committed))
            assert after == expected, (after,expected)
            connection.close()
            report['committed_restart_passed' if committed else 'uncommitted_rollback_passed'] = True
            before = after
        report['integrity_check'] = db.execute('PRAGMA integrity_check').fetchone()[0]
        assert report['integrity_check'] == 'ok'
        db.close()
    Path(__file__).with_name('storage-results.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))


if __name__ == '__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--crash':
        crash(sys.argv[2],bool(int(sys.argv[3])))
    main()
