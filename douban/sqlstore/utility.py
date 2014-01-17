#/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import cStringIO
import logging
import os
import re
import subprocess
import sys
import time

logger = logging.getLogger(__name__)
REVIEW_REQUEST_URL = "http://dbc.dapps.douban.com/review"
STATUS_CODE_100_RE = re.compile(r"^(?i)http/.*? 100 continue")
STATUS_CODE_RE = re.compile(r"^(?i)http/.*? (\d{3})")
TABLE_NAME_RE = re.compile(r"^\w{1,64}$")

def get_status_code(body):
    for line in body.splitlines():
        if not line or STATUS_CODE_100_RE.search(line):
            continue
        match = STATUS_CODE_RE.search(line)
        return match.group(1) if match else None

def dump(args):
    fn = "%s.tables.%s" % (args.database, time.time())
    mysqldump = subprocess.Popen(["mysqldump",
                                  "--compact",
                                  "-u", args.user,
                                  "-p%s" % args.password,
                                  "--host", args.host,
                                  "-B", args.database,
                                  "-d",
                                  "--tables"] + args.tables,
                                 stdout=open(fn, "w"))
    mysqldump.wait()

    if mysqldump.returncode != 0:
        try:
            os.remove(fn)
        except Exception:
            pass
        return mysqldump.returncode

    logger.info("Dumped to %s", fn)
    return 0

def review(args):
    ldap = args.ldap or os.getlogin()
    wlfpth = args.log_file
    schfpth = args.schema_file
    schfn = os.path.basename(schfpth)

    for t in args.tables:
        if not TABLE_NAME_RE.search(t):
            logger.error("%s isn't a valid table name.", t)
            return 1

    tables = ','.join(args.tables)
    assert os.path.isfile(wlfpth), "No such --log-file '%s'" % wlfpth
    assert os.path.isfile(schfpth), "No such --schema-file '%s'" % schfpth

    curl = "curl -i -X POST " \
           "-F wlf=@{wlfpth} " \
           "-F schf=@{schfpth} " \
           "-F tables={tbls} " \
           "-F schema={schfn} " \
           "-F ldap={ldap} " \
           "%s" % REVIEW_REQUEST_URL
    curl = curl.format(wlfpth=wlfpth, schfpth=schfpth, schfn=schfn,
                       tbls=tables, ldap=ldap)
    curl = curl.split()

    logger.debug("Post a review request to DBA.")
    p = subprocess.Popen(curl, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    so, se = p.communicate()

    if p.returncode != 0:
        logger.error(se)
        return p.returncode

    status = get_status_code(so)
    if status != "200":
        logger.error(so)
        return 1

    logger.debug(so)
    logger.info("Review request submitted.")
    return 0

def parse_args():

    def populate_review_sp(sp):
        sp.add_argument("-f", "--log-file", required=True,
                        help="SQL executed logging file, "
                        "It's saved at /tmp/$USER/ probably.")
        sp.add_argument("-s", "--schema-file", required=True,
                        help="Tables schema file, You could execute "
                        "'SHOW CREATE TABLE <tbl_name>' or `mysqldump`")
        sp.add_argument("-u", "--ldap", help="Your LDAP username, It's $USER "
                        " by default.")
        sp.add_argument("tables", nargs="+", help="Review tables")
        sp.set_defaults(subfunc=review)

    def populate_dump_sp(sp):
        sp.add_argument("-u", "--user", default="eye",
                        help="database user")
        sp.add_argument("-p", "--password", default="sauron",
                        help="database password")
        sp.add_argument("-H", "--host", default="localhost")
        sp.add_argument("-d", "--database", required=True)
        sp.add_argument("tables", nargs="+")
        sp.set_defaults(subfunc=dump)

    def gen_subparser(name, help=None):
        sp = sps.add_parser(name, help=help)
        sp.add_argument("-v", "--verbose", action="store_true")
        return sp

    parser = argparse.ArgumentParser()
    sps = parser.add_subparsers(title="commands", dest="subcommand")
    populate_review_sp(gen_subparser("review"))
    populate_dump_sp(gen_subparser("dump"))
    return parser.parse_known_args(sys.argv[1:])[0]

def main():
    primitive = logging.root.handlers
    logging.root.handlers = []

    try:
        args = parse_args()
        lv = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(level=lv, format='%(message)s')
        sys.exit(args.subfunc(args))
    finally:
        logging.root.handlers = primitive
