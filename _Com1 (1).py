
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


# In[ ]:

def fast_iterComments(context, cur,con):
    numbatch = 0
    counter = 0
    MAXINQUERY = 10000

    start_time = time.time()
    
    print "... START TIME at:" + str(start_time)

    query = "INSERT INTO comments(id, postid, score, text, creationdate, userdisplayname, userid) VALUES (%s, %s, %s, %s, %s, %s, %s);"
        
    for event, elem in context:
            
        counter+=1

        comment = ( elem.get("Id"), #"Id": 
                    elem.get("PostId") if elem.get("PostId") else None, 
                    elem.get("Score") if elem.get("Score") else None, 
                                        elem.get("Text") if elem.get("Text") else None, 
                        elem.get("CreationDate") if elem.get("CreationDate") else None,                                         
                    elem.get("UserDisplayName") if elem.get("UserDisplayName") else None,
                                        elem.get("UserId") if elem.get("UserId") else None
        )
        
        cur.execute(query,comment)

        del comment

        if(counter == MAXINQUERY or elem.getnext() == False):
            numbatch+=1
            counter = 0
            #print counter
            print "... commiting batch number " + str(numbatch) + ". Elapsed time: " + str(time.time() - start_time)
            con.commit()    

            
        
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]


# In[82]:


# In[ ]:

def main(type):
    
    con = None
    
    print "type: " + type
    
    if(type == "users"):
        infile = '/data/aditi/raw_data/stackoverflow/Users.xml'
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

        if(type == "users"):
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('users',))
            if(cur.fetchone()[0]):
                print "... table user existing"
                cur.execute("DROP TABLE users")
            
            cur.execute("CREATE TABLE users(id INT PRIMARY KEY, reputation INT, creationdate timestamp, displayname VARCHAR(50), lastaccessdate timestamp, location TEXT, aboutme TEXT, views INT, upvotes INT, downvotes INT, emailhash TEXT, lentext INT, hemo INT, semo INT, upperc FLOAT, punctu INT, code TEXT, hasurl TEXT, uurls TEXT, numurls INT, numgooglecode INT, numgithub INT, numtwitter INT, numlinkedin INT, numgoogleplus INT, numfacebook INT)")

        if(type == "votes"):
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('votes',))
            if(cur.fetchone()[0]):
                print "... table votes existing"
                cur.execute("DROP TABLE votes")             
            
            cur.execute("CREATE TABLE votes(id INT PRIMARY KEY, postid INT, votetypeid INT, userid INT, creationdate timestamp, bountyamount INT)")
            
        if(type == "comments"):
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('comments',))
            if(cur.fetchone()[0]):
                print "... table comments existing"
                cur.execute("DROP TABLE comments")              
            
            cur.execute("CREATE TABLE comments(id INT PRIMARY KEY, postid INT, score INT, Text TEXT, creationdate timestamp, userdisplayname TEXT, userid INT)")

        if(type == "badges"):
            cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ('badges',))
            if(cur.fetchone()[0]):
                print "... table badges existing"
                cur.execute("DROP TABLE badges")                
            
            cur.execute("CREATE TABLE badges(id INT PRIMARY KEY, userid INT, name TEXT, date timestamp)")


        con.commit()
        
        print "... table " + str(type) + " created"

        context = etree.iterparse(infile, events=('end',), tag='row')

        if(type == "users"):        
                questions = fast_iterUser(context, cur,con)
                print "... finished committing user data."  
                #print "... creating index"
                #cur.execute("CREATE INDEX users_id_index ON users (id);")
                con.commit()

      
        if(type == "votes"):        
                questions = fast_iterVotes(context, cur,con)
                print "... finished committing vote data."
                #print "... creating index"
                #cur.execute("CREATE INDEX id_index ON users (id);")    
                #cur.execute("CREATE INDEX votes_id_index ON votes (id);")
                con.commit()
                
        if(type == "comments"):     
                questions = fast_iterComments(context, cur,con)
                print "... finished committing comment data."   
                #print "... creating index"
                #cur.execute("CREATE INDEX id_index ON users (id);")    
                #cur.execute("CREATE INDEX comments_id_index ON comments (id);")
                con.commit()
                
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

