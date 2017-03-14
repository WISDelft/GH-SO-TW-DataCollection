
# coding: utf-8

# In[71]:

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


# In[74]:


def fast_iterUser(context, cur,con):
    numbatch = 0
    counter = 0
    MAXINQUERY = 10000
    start_time = time.time()
    print "... START TIME at:" + str(start_time)
    
    query = "INSERT INTO users (id, reputation, creationdate, displayname, lastaccessdate, location, aboutme, views, upvotes, downvotes, emailhash, lenText, upperc, punctu, hasurl, uurls, numurls, numgooglecode, numgithub, numtwitter, numlinkedin, numgoogleplus, numfacebook) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    urlsreg = "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
    
    tok = Tokenizer(preserve_case=False)

    for event, elem in context:
        counter+=1

        stripped = None
        lenText = 0
        hemo = 0
        semo = 0
        upper = 0
        punctu = 0
        code = False
        hasURL = False
        uurls = None
        numurls = 0
        numGoogleCode = 0
        numGitHub = 0
        numTwitter = 0
        numLinkedIn = 0
        numGooglePlus = 0
        numFacebook = 0

        try:
            if(elem.get("AboutMe")):
                stripped = elem.get("AboutMe").encode('utf-8','ignore')
                lenText = len(stripped) 
                tokenized = tok.tokenize(stripped)
                #hemo = sum(p in happyemoticons for p in tokenized)
                #semo =  sum(p in sademoticons for p in tokenized)
                upper =  float(sum(x.isupper() for x in stripped)) / float(len(stripped)) * 100
                punctu =  sum(o in string.punctuation for o in stripped)
                #code = True if sum(o in codetext for o in tokenized) > 0 else False
                result = re.findall(urlsreg, stripped)
                if(result):                 
                    uurls = ""
                    for u in result:
                        uurls = str(uurls) + str(u) + "|"
                        if "code.google" in u:
                            numGoogleCode += 1
                        if "plus.google" in u:
                            numGooglePlus += 1

                        if "twitter" in u:
                            numTwitter += 1

                        if "github" in u:
                            numGitHub += 1
                        if "linkedin" in u:
                            numLinkedIn += 1

                        if "facebook" in u:
                            numFacebook += 1

                    if(len(uurls) > 1):                     
                        uurls = uurls[:-1]
                
                    numurls = len(result)
                    if(numurls > 0):
                        hasURL = True

                del result
                del tokenized       


        except UnicodeDecodeError, e:
            print 'Error %s' % e    
        

        user = ( elem.get("Id"), #"Id": 
                elem.get("Reputation"), #"Reputation": 
                elem.get("CreationDate"), #"CreationDate": 
                elem.get("DisplayName"),    #"DisplayName": 
                elem.get("LastAccessDate"), #"LastAccessDate": 
                elem.get("Location"), #"Location": 
                stripped, #"AboutMe": 
                elem.get("Views"), #"Views": 
                elem.get("UpVotes"), #"UpVotes": 
                elem.get("DownVotes"), #"DownVotes": 
                elem.get("EmailHash"), #"EmailHash": 
                lenText,
                #hemo,
                #semo,
                upper,
                punctu,
                #code,
                hasURL,
                uurls,
                numurls,
                numGoogleCode,
                numGitHub,
                numTwitter,
                numLinkedIn,
                numGooglePlus,
                numFacebook
        )
        
        cur.execute(query,user)
        
        del user

        if(counter == MAXINQUERY or elem.getnext() == False):
            numbatch+=1
            counter = 0
            #print counter
            print "... commiting batch number " + str(numbatch) + ". Elapsed time: " + str(time.time() - start_time)
            con.commit();   
            
        
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
        #print user["Id"]


# In[83]:

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

        con.commit()
        
        print "... table " + str(type) + " created"

        context = etree.iterparse(infile, events=('end',), tag='row')

        if(type == "users"):        
                questions = fast_iterUser(context, cur,con)
                print "... finished committing user data."  
                #print "... creating index"
                #cur.execute("CREATE INDEX users_id_index ON users (id);")
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

