
# coding: utf-8

# In[ ]:




# In[ ]:




# In[ ]:

import requests
import tweepy
import psycopg2
import random
import sys
from PIL import Image

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
cursor = database.cursor()

    
cursor.execute("select loginname from GithubUsersPerData except select screen_name from t_gh_json")

log = cursor.fetchall()
#data = random.shuffle(log)
alpha = 0
ALPHA = []
nume = 0
NUME = []
sp = 0
SP = []
aln = 0
ALN = []
etc = 0
ETC = []
dash = 0
DASH = []
symbol = "~`!@#$%^&*()_+={}[]:>;',</?*+"
#print len(exGh)
print len(log)
#print len(data)
#print exGh
rand = []
alphabet = ""
numeric = ""
alphanumeric = ""
special = ""
d = ""
x = 0
#for i in range(0,4370576):
    #rand.append(random.choice(log))

#for i in range(0,500000):
#    rand.append(log[0])
#print log    
print "entering"
for i in log:
    #print i
    try:  
        ln = i[0]
        #print x = (x + 1)
        if ln.isalpha():
            alpha = alpha + 1
            ALPHA.append(ln)
            alphabet = alphabet + ln + ", "
            continue
        elif ln.isdigit():
            #nume = nume + 1
            #NUME.append(ln)
            #numeric = numeric + ln + ", "
            #print "numeric added"
            continue
        elif ln.isalnum():
            #aln = aln + 1 
            #ALN.append(ln)
            #alphanumeric = alphanumeric + ln + ", "
            continue
        else:
            for j in ln:
                if j in symbol:
                    #sp = sp + 1
                    #SP.append(ln)
                    #special = special + ln + ", "
                    break
                elif j == '-':
                    #dash = dash + 1
                    #DASH.append(ln)
                    #d = d + ln + ", "
                    break
            continue
    except:
        continue
    
count = alpha+nume+sp+aln+dash
print "count ",count 
print alpha
print nume
print aln
print sp
print dash
print "alpha : ",ALPHA
print "nume : ",NUME
print "aln : ",ALN
print "sp : ",SP
print "dash : ",DASH
alpha = 0
nume_u = 0
sp_u = 0
aln_u = 0
dash_u = 0  
useridlow = []
l = []
x = 0

delete = "DROP TABLE if exists WholeSearchGhAlphabet"
mydata = cursor.execute(delete)
cursor.execute("create table WholeSearchGhAlphabet(id TEXT, screenname TEXT, imgurl TEXT, login_input text)")
#cursor.execute("create table WholeSearchGhAlphanumeric(id TEXT, screenname TEXT, imgurl TEXT, login_input text)")
#cursor.execute("create table WholeSearchGhDash(id TEXT, screenname TEXT, imgurl TEXT, login_input text)")
#cursor.execute("create table WholeSearchGhSpecial(id TEXT, screenname TEXT, imgurl TEXT, login_input text)")


ranAlpha = alphabet
#ranNume = numeric
#ranAln = alphanumeric
#ranSp = special
#ranDash = d


def avhash(im):
    #print "in avhash"
    if not isinstance(im, Image.Image): 
        fil = cStringIO.StringIO(urllib.urlopen(im).read())
        im = Image.open(fil)
    im = im.resize((8, 8), Image.ANTIALIAS).convert('L')
    avg = reduce(lambda x, y: x + y, im.getdata()) / 64.
    return reduce(lambda x, (y, z): x | (z << y),
                  enumerate(map(lambda i: 0 if i < avg else 1, im.getdata())),
                  0)

def hamming(h1, h2):
    #print "in hamming"
    h, d = 0, h1 ^ h2
    while d:
        h += 1
        d &= d - 1
    return h

from bs4 import BeautifulSoup, NavigableString
from urllib2 import urlopen
print "now actual begins"
#Note: must be a public profile
found = 0
n = ""
for i in ranAlpha:
    if i == ',':
        user = n.strip()
        endpoint = "https://twitter.com/search?f=users&vertical=default&q=%s"
        f = urlopen(endpoint % user)
        html =  f.read()
        f.close()
        soup =  BeautifulSoup(html, 'html.parser') 
        tweets =  soup.find_all('div', 'GridTimeline')
        nofounduser = 0
        nogrids = 0
        
        for timelines in tweets:
            grids = timelines.find_all('div', 'Grid')
            for val in grids:
                nogrids = nogrids + 1
                ac = val.find_all('div', 'Grid-cell')
                for b in ac:
                    nofounduser = nofounduser + 1
                    profile = b.find('div', 'ProfileCard')
                    screen_name = profile['data-screen-name']
                    idno = profile['data-user-id']
                    #print "SCREENNAME : ",screen_name
                    #print "ID : ",idno
                    profContent = b.find('div', 'ProfileCard-content')
                    t = profContent.find('a', 'ProfileCard-avatarLink')
                    name = t['title']
                    #print "NAME : ",name
                    j = t.find('img', 'ProfileCard-avatarImage')
                    imgurl = j['src']
                    cursor.execute("select websiteurls ,loginname, profileimageurl from githubusersperdata where loginname = '%s'"%n)
                    w = cursor.fetchall()
                    flag = 0
                    if imgurl != None:         
                        for j in range(0,len(w)):
                            if w[j][2] != None:
                                img1 = imgurl
                                img2 = w[j][2]
                                #print "till here"
                                try:
                                    hash1 = avhash(img1)
                                    hash2 = avhash(img2)
                                    dist = hamming(hash1, hash2)
                                except IOError as e:
                                    pass
                                #print "hash(%s) = %d\nhash(%s) = %d\nhamming distance = %d\nsimilarity = %d%%" % (img1, hash1, img2, hash2, dist, (64 - dist) * 100 / 64)
                                similarity = (64 - dist) * 100 / 64
                                if hash2 != 17027477209495552:
                                    cat = "Image + Search + alphabet + gh"
                                    if similarity >= 80 and similarity <90 and len(w[j][1]) > 9:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhAlphabet(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 1"
                                        flag = 1
                                    elif similarity >90 and similarity <98 and len(w[j][1]) > 7:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhAlphabet(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 2"
                                        flag = 1
                                    elif similarity >= 98:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhAlphabet(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 3"
                                        flag = 1
                            else:
                                continue
        if flag == 0: #If loginname not matched then use name as input
            cursor.execute("select name from githubusersperdata where loginname = '%s'"%n)
            n = cursor.fetchall()
                        
        
        
        #print nofounduser
        #print nogrids
        n = ""
        if nofounduser >= 1:
            found = found + 1
    else:
        n = n + i
database.commit()
print "final : alpha - ",found
print "accurate matched a : ",accmatch
'''
## Alphanumeric

found = 0
n = ""
for i in ranAln:
    if i == ',':
        user = n.strip()
        endpoint = "https://twitter.com/search?f=users&vertical=default&q=%s"
        f = urlopen(endpoint % user)
        html =  f.read()
        f.close()
        soup =  BeautifulSoup(html, 'html.parser') 
        tweets =  soup.find_all('div', 'GridTimeline')
        nofounduser = 0
        nogrids = 0
        for timelines in tweets:
            grids = timelines.find_all('div', 'Grid')
            for val in grids:
                nogrids = nogrids + 1
                ac = val.find_all('div', 'Grid-cell')
                for b in ac:
                    nofounduser = nofounduser + 1
                    profile = b.find('div', 'ProfileCard')
                    screen_name = profile['data-screen-name']
                    idno = profile['data-user-id']
                    #print "SCREENNAME : ",screen_name
                    #print "ID : ",idno
                    profContent = b.find('div', 'ProfileCard-content')
                    t = profContent.find('a', 'ProfileCard-avatarLink')
                    name = t['title']
                    #print "NAME : ",name
                    j = t.find('img', 'ProfileCard-avatarImage')
                    imgurl = j['src']
                    cursor.execute("select websiteurls ,loginname, profileimageurl from githubusersperdata where loginname = '%s'"%n)
                    w = cursor.fetchall()
                    if imgurl != None:         
                        for j in range(0,len(w)):
                            if w[j][2] != None:
                                img1 = imgurl
                                img2 = w[j][2]
                                #print "till here"
                                try:
                                    hash1 = avhash(img1)
                                    hash2 = avhash(img2)
                                    dist = hamming(hash1, hash2)
                                except IOError as e:
                                    pass
                                #print "hash(%s) = %d\nhash(%s) = %d\nhamming distance = %d\nsimilarity = %d%%" % (img1, hash1, img2, hash2, dist, (64 - dist) * 100 / 64)
                                similarity = (64 - dist) * 100 / 64
                                if hash2 != 17027477209495552:
                                    cat = "Image + Search + alphabet + gh"
                                    if similarity >= 80 and similarity <90 and len(w[j][1]) > 9:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhAlphanumeric(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 1"
                                    elif similarity >90 and similarity <98 and len(w[j][1]) > 7:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhAlphanumeric(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 2"
                                    elif similarity >= 98:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhAlphanumeric(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 3"
                            else:
                                continue
        #print nofounduser
        #print nogrids
        n = ""
        if nofounduser >= 1:
            found = found + 1
    else:
        n = n + i
database.commit()
print "final : alphanumeric - ",found
print "accurate matched aln : ",accmatch

## Dash

found = 0
n = ""
for i in ranDash:
    if i == ',':
        user = n.strip()
        endpoint = "https://twitter.com/search?f=users&vertical=default&q=%s"
        f = urlopen(endpoint % user)
        html =  f.read()
        f.close()
        soup =  BeautifulSoup(html, 'html.parser') 
        tweets =  soup.find_all('div', 'GridTimeline')
        nofounduser = 0
        nogrids = 0
        for timelines in tweets:
            grids = timelines.find_all('div', 'Grid')
            for val in grids:
                nogrids = nogrids + 1
                ac = val.find_all('div', 'Grid-cell')
                for b in ac:
                    nofounduser = nofounduser + 1
                    profile = b.find('div', 'ProfileCard')
                    screen_name = profile['data-screen-name']
                    idno = profile['data-user-id']
                    #print "SCREENNAME : ",screen_name
                    #print "ID : ",idno
                    profContent = b.find('div', 'ProfileCard-content')
                    t = profContent.find('a', 'ProfileCard-avatarLink')
                    name = t['title']
                    #print "NAME : ",name
                    j = t.find('img', 'ProfileCard-avatarImage')
                    imgurl = j['src']
                    cursor.execute("select websiteurls ,loginname, profileimageurl from githubusersperdata where loginname = '%s'"%n)
                    w = cursor.fetchall()
                    if imgurl != None:         
                        for j in range(0,len(w)):
                            if w[j][2] != None:
                                img1 = imgurl
                                img2 = w[j][2]
                                #print "till here"
                                try:
                                    hash1 = avhash(img1)
                                    hash2 = avhash(img2)
                                    dist = hamming(hash1, hash2)
                                except IOError as e:
                                    pass
                                #print "hash(%s) = %d\nhash(%s) = %d\nhamming distance = %d\nsimilarity = %d%%" % (img1, hash1, img2, hash2, dist, (64 - dist) * 100 / 64)
                                similarity = (64 - dist) * 100 / 64
                                if hash2 != 17027477209495552:
                                    cat = "Image + Search + alphabet + gh"
                                    if similarity >= 70 and similarity <90 and len(w[j][1]) > 13:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhDash(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 1"
                                    elif similarity >90 and similarity <98 and len(w[j][1]) > 10:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhDash(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 2"
                                    elif similarity >= 98:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhDash(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 3"
                            else:
                                continue
        #print nofounduser
        #print nogrids
        n = ""
        if nofounduser >= 1:
            found = found + 1
    else:
        n = n + i
database.commit()
print "final : dash - ",found
print "accurate matched dash : ",accmatch


##Special

found = 0
n = ""
for i in ranSp:
    if i == ',':
        user = n.strip()
        endpoint = "https://twitter.com/search?f=users&vertical=default&q=%s"
        f = urlopen(endpoint % user)
        html =  f.read()
        f.close()
        soup =  BeautifulSoup(html, 'html.parser') 
        tweets =  soup.find_all('div', 'GridTimeline')
        nofounduser = 0
        nogrids = 0
        for timelines in tweets:
            grids = timelines.find_all('div', 'Grid')
            for val in grids:
                nogrids = nogrids + 1
                ac = val.find_all('div', 'Grid-cell')
                for b in ac:
                    nofounduser = nofounduser + 1
                    profile = b.find('div', 'ProfileCard')
                    screen_name = profile['data-screen-name']
                    idno = profile['data-user-id']
                    #print "SCREENNAME : ",screen_name
                    #print "ID : ",idno
                    profContent = b.find('div', 'ProfileCard-content')
                    t = profContent.find('a', 'ProfileCard-avatarLink')
                    name = t['title']
                    #print "NAME : ",name
                    j = t.find('img', 'ProfileCard-avatarImage')
                    imgurl = j['src']
                    cursor.execute("select websiteurls ,loginname, profileimageurl from githubusersperdata where loginname = '%s'"%n)
                    w = cursor.fetchall()
                    if imgurl != None:         
                        for j in range(0,len(w)):
                            if w[j][2] != None:
                                img1 = imgurl
                                img2 = w[j][2]
                                #print "till here"
                                try:
                                    hash1 = avhash(img1)
                                    hash2 = avhash(img2)
                                    dist = hamming(hash1, hash2)
                                except IOError as e:
                                    pass
                                #print "hash(%s) = %d\nhash(%s) = %d\nhamming distance = %d\nsimilarity = %d%%" % (img1, hash1, img2, hash2, dist, (64 - dist) * 100 / 64)
                                similarity = (64 - dist) * 100 / 64
                                if hash2 != 17027477209495552:
                                    cat = "Image + Search + alphabet + gh"
                                    if similarity >= 80 and similarity <90 and len(w[j][1]) > 9:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhSpecial(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 1"
                                    elif similarity >90 and similarity <98 and len(w[j][1]) > 7:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhSpecial(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 2"
                                    elif similarity >= 98:
                                        accmatch = accmatch + 1
                                        cursor.execute("insert into WholeSearchGhSpecial(id, screenname, imgurl, login_input) values(%s,%s,%s,%s)",(idno, screen_name, img_url, n)) 
                                        print "inserted image 3"
                            else:
                                continue
        #print nofounduser
        #print nogrids
        n = ""
        if nofounduser >= 1:
            found = found + 1
    else:
        n = n + i
print "final : special - ",found
print "accurate matched sp: ",accmatch
'''
cursor.close()
database.commit()
database.close()

