#!/usr/bin/env python

from unittest import TestCase
from douban.sqlstore.table_finder import find_tables

class ModuleTest(TestCase):
    def test_find_tables_in_select(self):
        sql = '''select group.id,group_member.status,group.type from `group`,group_member where group.id=group_member.group_id and group_member.user_id='44731395' order by lastday desc'''
        self.assertEqual(set(['group', 'group_member']), find_tables(sql))

    def test_find_tables_in_select_with_aliases(self):
        sql = '''select g.id,gm.status,g.type from group g,group_member gm where g.id=gm.group_id and gm.user_id='44731395' order by gm.lastday desc'''
        self.assertEqual(set(['group', 'group_member']), find_tables(sql))

    def test_find_tables_in_select_with_join(self):
        sql = '''select a.group_id from group_subject a left join `group` b on a.group_id = b.id where b.type <> _latin1'R' and a.subject_id='2811687' order by `time` desc limit 0,300'''
        self.assertEqual(set(['group_subject', 'group']), find_tables(sql))

    def test_find_tables_in_delete(self):
        sql = 'delete from email_outbox where id=261815597'
        self.assertEqual(set(['email_outbox']), find_tables(sql))

    def test_find_tables_in_update(self):
        sql = "update review_stats set read_count=read_count+1 where review_id='4615068'"
        self.assertEqual(set(['review_stats']), find_tables(sql))

    def test_find_tables_in_replace(self):
        sql = "replace into large_icon(user_id, link) values('33964480', 'ul33964480.jpg')"
        self.assertEqual(set(['large_icon']), find_tables(sql))

    def test_find_tables_in_insert(self):
        sql = "insert into update_log (type, item_id, extra_info) values (10, '2209058', '')"
        self.assertEqual(set(['update_log']), find_tables(sql))
