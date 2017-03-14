__author__ = 'Giuseppe Silvestri'

import requests
import sys
import psycopg2
import re
import warnings
import pprint
import time
from random import randint
from PIL import Image
import cStringIO
import urllib
import time
import csv
import pprint
import time
import datetime
import pprint
import calendar

warnings.filterwarnings('ignore')

def repo_to_db(repo_file,newtable):
    repo_list = _get_repo_list(repo_file)
    _repos_into_database(repo_list,newtable)

def _get_repo_list(repo_file):
    repos = []
    mur = csv.reader(open(repo_file,"rb"))
    #discard the first header row
    i = 0
    for row in mur:
        if i > 0:
            repos.append((row[0],row[1],row[2],row[3],row[4],row[5],row[6])) #(gh_user_id,gh_user_loginname,repo_name,repo_description,repo_language,repo_create_time,repo_update_time)
        i+=1
    print "#repos rows = "+str(len(repos))

    return repos

def _repos_into_database(repo_list,newtable):
    cur.execute("select exists (select * from information_schema.tables where table_name=%s)", ('github_64k_users_repos_info',))
    if(cur.fetchone()[0]):
        print "[GithubFollowToDb] table github_64k_users_repos_info existing in DB"
        if newtable == 1:
            print '[GithubFollowToDb] create new github_64k_users_repos_info'
            cur.execute('DROP TABLE github_64k_users_repos_info')
            cur.execute('CREATE TABLE github_64k_users_repos_info ('
                                         "gh_user_id INT,"
                                         "gh_user_loginname TEXT, "
					 "repo_name TEXT, "
					 "repo_description TEXT, "
					 "repo_language TEXT, "
					 "repo_create_time timestamp, "
					 "repo_update_time timestamp, "
                                         "PRIMARY KEY(gh_user_id,repo_name) "
                        ");")
        con.commit()
    else:
        cur.execute('CREATE TABLE github_64k_users_repos_info ('
                                         "gh_user_id INT,"
                                         "gh_user_loginname TEXT, "
                                         "repo_name TEXT, "
                                         "repo_description TEXT, "
                                         "repo_language TEXT, "
                                         "repo_create_time timestamp, "
                                         "repo_update_time timestamp, "
                                         "PRIMARY KEY(gh_user_id,repo_name) "
                        ");")
        con.commit()
    #the query ignores duplicate entries (row with the same primary key)

    queryinsert = "INSERT INTO github_64k_users_repos_info (gh_user_id,gh_user_loginname,repo_name,repo_description,repo_language,repo_create_time,repo_update_time)" \
                  " SELECT %s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS(SELECT * FROM github_64k_users_repos_info  " \
                         "                 WHERE gh_user_id = %s AND repo_name = %s);"
    rowcnt = 0           #test

    for r in repo_list:
        userinsert = (
            r[0],
            r[1],
            r[2],
            r[3],
	    r[4],
	    r[5],
	    r[6],
	    r[0],
	    r[2],
        )
        cur.execute(queryinsert,userinsert)
        rowcnt = rowcnt + 1
	if rowcnt % 1000 == 0:
	    print "# inserted "+str(rowcnt)
        del userinsert

    print "[GithubFollowToDb] done."
    con.commit()

def _db_connection():

    global cur,con
    con = None
    try:
        con = psycopg2.connect(database='so_gh_tw_usermatching', user='postgres', password='postgres')
        cur = con.cursor()
        cur.execute('SELECT version()')
        ver = cur.fetchone()
        print "Connecting to DB: " + str(ver)
    except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

def main(repo_file,newtable):
    _db_connection()
    repo_to_db(repo_file,newtable)

if __name__ == '__main__':
    repo_file = sys.argv[1]
    newtable = int(sys.argv[2])
    main(repo_file,newtable)

