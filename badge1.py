
# coding: utf-8

# In[ ]:


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

def fast_iterBadges(context, cur,con):
    numbatch = 0
    counter = 0
    MAXINQUERY = 10000

    start_time = time.time()
    
    print "... START TIME at:" + str(start_time)

    query = "INSERT INTO badges (id, userid, name, date) VALUES (%s, %s, %s, %s)"
        
    for event, elem in context:
            
        counter+=1

        badge = ( elem.get("Id"), #"Id": 
                    elem.get("UserId") if elem.get("UserId") else None, 
                    elem.get("Name") if elem.get("Name") else None, 
                    elem.get("Date") #if elem.get("CreationDate") else None
        )
        
        cur.execute(query,badge)

        del badge

        if(counter == MAXINQUERY or elem.getnext() == False):
            numbatch+=1
            counter = 0
            #print counter
            print "... commiting batch number " + str(numbatch) + ". Elapsed time: " + str(time.time() - start_time)
            con.commit()    

            
        
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


# In[83]:

def main(type):
    
    con = None
    
    print "type: " + type
    
    if(type == "badges"):
        infile = '/data/aditi/raw_data/stackoverflow/Badges.xml'

    try:
        #CAMBIA CAMBIA CAMBIA CAMBIA
        con = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
        cur = con.cursor()
        cur.execute('SELECT version()')          
        ver = cur.fetchone()
        print "Connecting to DB: " + str(ver)


        if(type == "badges"):
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('badges',))
            if(cur.fetchone()[0]):
                print "... table badges existing"
                cur.execute("DROP TABLE badges")                
            
            cur.execute("CREATE TABLE badges(id INT PRIMARY KEY, userid INT, name TEXT, date timestamp)")


        con.commit()
        
        print "... table " + str(type) + " created"

        context = etree.iterparse(infile, events=('end',), tag='row')

                
        if(type == "badges"):       
                questions = fast_iterBadges(context, cur,con)
                print "... finished committing badge data." 
                #print "... creating index"
                #cur.execute("CREATE INDEX id_index ON users (id);")    
                #cur.execute("CREATE INDEX badges_id_index ON badges (id);")
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

