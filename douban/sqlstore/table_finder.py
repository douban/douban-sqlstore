#!/usr/bin/env python

import re

SQL_PATTERNS = {
    'select': re.compile(r'select.*?\s+from\s+|\swhere\s.*|\sjoin\s|\susing\s', re.I|re.S),
    'insert': re.compile(r'insert\s+(ignore\s+)?(into\s+)?`?(?P<table>\w+)`?', re.I),
    'update': re.compile(r'update\s+(ignore\s+)?|\sset\s.*', re.I|re.S),
    'replace': re.compile(r'replace\s+(into\s+)?`?(?P<table>\w+)`?', re.I),
    'delete': re.compile(r'delete.*?from\s+|\swhere\s.*', re.I|re.S),
}
re_cleanup = re.compile(r'[^\w\s,]')
re_table = re.compile(r'(?:^|,)\s*(\w+)')
re_from = re.compile('\sfrom\s', re.I)
def find_tables(sql):
    cmd = sql.split(' ', 1)[0].lower()
    if cmd in ['select', 'update', 'delete']:
        if cmd == 'select' and not re_from.search(sql):
            return set()
        table_refs = SQL_PATTERNS[cmd].split(sql)
        tables = [re_table.findall(re_cleanup.sub('', tr)) for tr in table_refs if tr]
        return set(sum(tables, []))
    elif cmd in ['insert', 'replace']:
        match = SQL_PATTERNS[cmd].match(sql)
        if match:
            return set([match.group('table')])
        else:
            return set()
    else:
        return set()

# vim: set et ts=4 sw=4 :
