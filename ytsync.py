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
    default=None
)

parser.add_argument(
    'action',
    action='store',
    help='Action',
    choices=['create', 'insert', 'delete', 'sources', 'videos', 'sync', 'get', 'input', 'output', 'query'],
)

parser.add_argument(
    'url',
    action='store',
    nargs='?',
    help='Unique Url',
    default=None
)

parser.add_argument(
    '-o',
    '--output',
    action='store',
    nargs='?',
    help='Output template',
    default='%(extractor)s/%(uploader)s/%(title)s-%(id)s.%(ext)s'
)

parser.add_argument(
    '-f',
    '--fetch',
    action='store_true',
    required=False,
    help='Fetch sources',
)

parser.add_argument(
    '-d',
    '--download',
    action='store_true',
    required=False,
    help='Download videos',
)

if len(sys.argv) < 2:
    parser.print_help()
    sys.exit(0)

args = parser.parse_args()

'''
if not args.path and args.action in ['insert', 'update', 'delete', 'sources', 'videos', 'sync', 'get']:
    error('Error: Missing argument - path (-p)')
    sys.exit(1)
'''
if not args.url and args.action in ['insert', 'delete', 'input', 'output', 'query', 'get']:
    error('Error: Missing argument - [url]')
    sys.exit(1)

if args.action == 'sync' and not (args.fetch or args.download):
    error('Must specify at least one of [fetch|download] with sync (-f, -d)')
    sys.exit(1)

""" Prepare the database """
db = ytsync.Database('sqlite:///' + os.path.expanduser(args.path), log, echo=False)

""" Client controls sync parameters """
ydl_opts = {
    'outtmpl': args.output,
    'logger': log,
    'extract_flat': 'in_playlist',
}

if args.action == 'insert':
    db.insert(args.url, timedelta(days=1))
elif args.action == 'delete':
    if not db.delete(args.url):
        error('Error: No records found')
        exit(1)
elif args.action == 'sources':
    for source in db.sources():
        debug(source)
    exit(0)
elif args.action == 'videos':
    for video in db.videos():
        debug(video)
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
    db.sync(ydl_opts, url=args.url, fetch=args.fetch, download=args.download)
elif args.action == 'get':
    db.get(ydl_opts, args.url)

try:
    db.session.commit()
    debug('Command Successful')
except IntegrityError:
    error('Error: IntegrityError... duplicate entity?')
    db.session.rollback()
    exit(1)
