#!/usr/bin/python2.7
from __future__ import unicode_literals

import argparse
import json
import os
import sys
import ytsync

from datetime import timedelta
from sqlalchemy.exc import IntegrityError

class MyLogger(object):
    @staticmethod
    def debug(msg):
        sys.stderr.write(msg)
        sys.stderr.write('\n')

    @staticmethod
    def warning(msg):
        sys.stderr.write(msg)
        sys.stderr.write('\n')

    @staticmethod
    def error(msg):
        sys.stderr.write(msg)
        sys.stderr.write('\n')

log = MyLogger()

def progress(d):
    log.debug(d)

def debug(d):
    log.debug(d)

def error(msg):
    log.error('>>> ERROR: ' + msg)

def dump(msg):
    sys.stdout.write(json.dumps(msg, ensure_ascii=False).encode('utf8'))
    sys.stdout.write('\n')

parser = argparse.ArgumentParser(
    description='SyncDB Shell Tool',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    'path',
    action='store',
    help='Database Path',
)
parser.add_argument(
    'action',
    action='store',
    help='Action',
    choices=['create', 'insert', 'update', 'delete', 'sources', 'query', 'sync', 'input', 'output'],
)
parser.add_argument(
    'url',
    action='store',
    nargs='?',
    help='Unique Url',
    default=None
)

if (len(sys.argv)) == 0:
    parser.print_help()
    sys.exit(0)

args = parser.parse_args()

if args.action in ['insert', 'update', 'delete', 'input', 'output', 'query'] and args.url is None:
    error('Error: Missing argument - (item)')
    sys.exit(1)

""" Prepare the database """
db = ytsync.Database('sqlite:///' + os.path.expanduser(args.path), log, echo=False)

""" Client controls sync parameters """
ydl_opts = {
    'outtmpl': '%(id)s%(ext)s',
    'logger': log,
    'progress_hooks': [progress],
    'extract_flat': 'in_playlist'
}

if args.action == 'insert':
    if not db.insert(args.url, timedelta(days=1)):
        error('Error: No suitable extractors found')
        exit(1)
elif args.action == 'update':
    if not db.update(args.url, ydl_opts):
        error('Error: No records found')
        exit(1)
elif args.action == 'delete':
    if not db.delete(args.url):
        error('Error: No records found')
        exit(1)
elif args.action == 'sources':
    for source in db.sources():
        debug(source)
    exit(0)
elif args.action == 'input':
    debug(db.input(args.url))
    exit(0)
elif args.action == 'output':
    debug(db.output(args.url))
    exit(0)
elif args.action == 'query':
    for item in db.query(args.url, ydl_opts):
        dump(item)
elif args.action == 'sync':
    if not db.sync(args.url):
        error('Error: Possible parser error')
        exit(1)

try:
    db.session.commit()
    debug('Command Successful')
except IntegrityError:
    error('Error: IntegrityError... duplicate source/video or null value?')
    db.session.rollback()
    exit(1)
