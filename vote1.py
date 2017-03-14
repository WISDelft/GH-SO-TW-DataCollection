from lxml import etree
import psycopg2
import sys
import os
from HTMLParser import HTMLParser
from happyfuntokenizing import *
import re
import json
import string
import time
from functools import wraps


# In[72]:

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)


# In[73]:

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def fast_iterVotes(context, cur,con):
    numbatch = 0
    counter = 0
    MAXINQUERY = 10000

    start_time = time.time()
    
    print "... START TIME at:" + str(start_time)

    query = "INSERT INTO votes (id, postid, votetypeid, userid, creationdate, bountyamount) VALUES (%s, %s, %s, %s, %s, %s)"
        
    for event, elem in context:
        
        if(elem.get("VoteTypeId") != "1" and elem.get("VoteTypeId") != "2" and elem.get("VoteTypeId") != "3"):  #only posts voted to be closed
                        #print elem.get("VoteTypeId")
            elem.clear()
            continue
            
            counter+=1

        vote = ( elem.get("Id"), #"Id": 
                    elem.get("PostId") if elem.get("PostId") else None, #"PostId": 
                    elem.get("VoteTypeId"), #"VoteTypeId":
                                        elem.get("UserId") if elem.get("UserId") else None, #"UserId":
                        elem.get("CreationDate") if elem.get("CreationDate") else None, #"CreationDate":                                        
                    elem.get("BountyAmount") if elem.get("BountyAmount") else None, #"Comment"                                      
        )
                
        cur.execute(query,vote)

        del vote

        if(counter == MAXINQUERY or elem.getnext() == False):
            numbatch+=1
            counter = 0
            #print counter
            print "... commiting batch number " + str(numbatch) + ". Elapsed time: " + str(time.time() - start_time)
            con.commit()    

            
        
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        #print user["Id"]
        
def main(type):
    
    con = None
    
    print "type: " + type
    
    if(type == "votes"):
        infile = '/data/aditi/raw_data/stackoverflow/Votes.xml'
    if(type == "comments"):
        infile = '/data/aditi/raw_data/stackoverflow/Comments.xml'
    if(type == "badges"):
        infile = '/data/aditi/raw_data/stackoverflow/Badges.xml'

    try:
        #CAMBIA CAMBIA CAMBIA CAMBIA
        con = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
        cur = con.cursor()
        cur.execute('SELECT version()')          
        ver = cur.fetchone()
        print "Connecting to DB: " + str(ver)
       
        if(type == "votes"):
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('votes',))
            if(cur.fetchone()[0]):
                print "... table votes existing"
                cur.execute("DROP TABLE votes")             
            
            cur.execute("CREATE TABLE votes(id INT PRIMARY KEY, postid INT, votetypeid INT, userid INT, creationdate timestamp, bountyamount INT)")
          
        con.commit()
        
        print "... table " + str(type) + " created"

        context = etree.iterparse(infile, events=('end',), tag='row')

        if(type == "votes"):        
                questions = fast_iterVotes(context, cur,con)
                print "... finished committing vote data."
                #print "... creating index"
                #cur.execute("CREATE INDEX id_index ON users (id);")    
                #cur.execute("CREATE INDEX votes_id_index ON votes (id);")
                con.commit()
                          
        
        print "... done with indexing"

    except psycopg2.DatabaseError, e:
            print 'Error %s' % e    
            sys.exit(1)
    
    finally:
            if con:
                    con.close()
    


# In[84]:

if __name__ == '__main__':
    #types: user, questions, answers, votes, comments, closedquestionhistory, closedquestionhistory2 (edit), closedquestionhistory3 (orign)
    type = sys.argv[1]
    main(type)

