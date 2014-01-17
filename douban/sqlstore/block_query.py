#!/usr/bin/env python

import re
import time
import pickle
import argparse
from hashlib import md5

from douban.cfgmanager import cfgpusher_from_config

CFGPUSHER_CONFIG = 'douban-online'
BLACKLIST_NODE = '/mysql/sqlstore-blacklist'

def main():
    """Entry point"""

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='commands', dest='command')

    block_parser = subparsers.add_parser(name='block')
    block_parser.add_argument('-t', '--block-time', type=int, default=60,
                              metavar='SECONDS',
                              help=('How long in seconds to block the query, '
                                    '60 seconds by default'))
    block_parser.add_argument('query', metavar='SQL|MD5',
                              help=('Query to block, identified by '
                                    'full query or md5'))

    unblock_parser = subparsers.add_parser(name='unblock')
    unblock_parser.add_argument('query', metavar='SQL|MD5',
                                help=('Query to block, identified by full '
                                      'query or md5'))

    args = parser.parse_args()

    if re.match('[a-z0-9]{32}', args.query):
        _type = 'partial'
        digest = args.query
    else:
        _type = 'full'
        digest = md5(args.query).hexdigest()

    blacklist = {
        'full': {},
        'partial': {},
    }

    if args.command == 'block':
        block_until = time.time() + args.block_time
        blacklist[_type] = {digest: block_until}
        message = ('All queries with digest {} are blocked, '
                   'and will be unblocked in {} seconds (after {})')
        message = message.format(digest,
                                 args.block_time,
                                 time.ctime(block_until))
    elif args.command == 'unblock':
        blacklist[_type] = {digest: -1}
        message = 'All queries with digest {} are unblocked'
        message = message.format(digest)

    pusher = cfgpusher_from_config(CFGPUSHER_CONFIG)
    pusher.push(BLACKLIST_NODE, pickle.dumps(blacklist))

    print message
    return 0

if __name__ == '__main__':
    main()
