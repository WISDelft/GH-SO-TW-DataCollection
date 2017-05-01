
# coding: utf-8

# In[143]:

import psycopg2
import sys
from PIL import Image
import urllib, cStringIO

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
cursor = database.cursor()

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
#print "one "

if __name__ == '__main__':
    #delete = "DROP TABLE if exists WholeLookupGhNume"
    #mydata = cursor.execute(delete)
    #cursor.execute("create table WholeLookupGhNume(id text, screen_name text, name text, created_at text, url text, followers_count text, friends_count text, statuses_count text, favourites_count text, listed_count text, contributors_enabled text, profile_image_url text, description text, protected text, location text, lang text, expanded_url text, category text)")
    accmatch = 0
    found = 0
    
    ranNume = " 9902468 ,  9903148266 ,  9903286 ,  9904050270 ,  9906 ,  990645487 ,  9908 ,  9910265639 ,  99103112 ,  991060 ,  99133799 ,  991377965 ,  99154562 ,  99155012 ,  991899783 ,  992 ,  992161809 ,  992274880 ,  9923 ,  9923495127 ,  9924521284 ,  99250 ,  9925420 ,  9929105 ,  992910624 ,  993 ,  9930125 ,  9932551 ,  9937391140 ,  993909705 ,  9939781 ,  994125010 ,  994357338 ,  9944990 ,  994724435 ,  994811089 ,  994955138 ,  9951219 ,  995227004 ,  995533 ,  9955870 ,  995795498 ,  9958 ,  995813528 ,  9958708006 ,  9962202 ,  996268132 ,  996465114 ,  99662954 ,  99692957 ,  996945978 ,  997 ,  9970 ,  99703005 ,  997164200 ,  997204035 ,  9972103177 ,  99731 ,  997373225 ,  9974 ,  9975 ,  99815574 ,  99848873 ,  998800 ,  99881 ,  9989838 ,  99901919 ,  99904047 ,  999134 ,  9991345 ,  99922 ,  999273 ,  9992800 ,  9994092 ,  9995461 ,  99989796950 ,  9999472 ,  99999999995 ,"
    
    modAlpha = ""
    for i in ranNume:
        if i == "'":
            continue
        elif i == "-":
            modAlpha = modAlpha + " "
        else:
            modAlpha = modAlpha + i
    
    from twython import Twython 

    t = Twython(app_key='Q1JCm8sNWZacaEr8mL4PuMIdY', #REPLACE 'APP_KEY' WITH YOUR APP KEY, ETC., IN THE NEXT 4 LINES
        app_secret='uKU1R1oMbRFZwDraQ4DuhUcocSCyxu3XhQhNdq8zcLM7UnREXT',
        oauth_token='858676481253404672-CMzOK4Zq2QLMTMcQrg17lpmBVmQAleQ',
        oauth_token_secret='I7LP4RaWkidBZ4Yj3wExqPyfoOkpOIMyHvqd142Hn8sjk')
    
    users = t.lookup_user(screen_name = ranNume)
    
    fields = "id screen_name name created_at url followers_count friends_count statuses_count     favourites_count listed_count     contributors_enabled profile_image_url description protected location lang expanded_url".split()
    
    for entry in users:
        #CREATE EMPTY DICTIONARY
        r = {}
        for f in fields:
            r[f] = ""
        #ASSIGN VALUE OF 'ID' FIELD IN JSON TO 'ID' FIELD IN OUR DICTIONARY
        r['id'] = entry['id']
        r['screen_name'] = entry['screen_name']
        r['name'] = entry['name']
        r['created_at'] = entry['created_at']
        r['url'] = entry['url']
        r['followers_count'] = entry['followers_count']
        r['friends_count'] = entry['friends_count']
        r['statuses_count'] = entry['statuses_count']
        r['favourites_count'] = entry['favourites_count']
        r['listed_count'] = entry['listed_count']
        r['contributors_enabled'] = entry['contributors_enabled']
        r['profile_image_url'] = entry['profile_image_url']
        r['description'] = entry['description']
        r['protected'] = entry['protected']
        r['location'] = entry['location']
        r['lang'] = entry['lang']
        #NOT EVERY ID WILL HAVE A 'URL' KEY, SO CHECK FOR ITS EXISTENCE WITH IF CLAUSE
        if 'url' in entry['entities']:
            r['expanded_url'] = entry['entities']['url']['urls'][0]['expanded_url']
        else:
            r['expanded_url'] = ''
        found = found + 1
        cursor.execute("select websiteurls,loginname, profileimageurl from githubusersperdata where loginname = '%s'"%r['screen_name'])
        w = cursor.fetchall()
        flag = 0
        if r['url'] != None:
        #    #cursor.execute("select websiteurls,loginname, profile_image_url from githubusersperdata where loginname = '%s'"%p2000[i][2])
            print "web started"
            for web in range(0,len(w)):
                if w[web][0] != '':
                    if "%facebook.com%" in w[web][0] or "%facebook.com%" in r['url'] or "%instagram%" in w[web][0] or "%instagram%" in r['url']:
                        continue
                    else:
                        if w[web][0] == r['url']:
                            accmatch = accmatch + 1
                            cat = "Website + Lookup + numeric + so"
                            cursor.execute("insert into WholeLookupSoNume(id, screen_name, name, created_at, url, followers_count, friends_count, statuses_count, favourites_count, listed_count, contributors_enabled, profile_image_url, description, protected, location, lang, expanded_url, category) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(r['id'], r['screen_name'], r['name'], r['created_at'], r['url'], r['followers_count'], r['friends_count'], r['statuses_count'], r['favourites_count'], r['listed_count'], r['contributors_enabled'], r['profile_image_url'], r['description'], r['protected'], r['location'], r['lang'], r['expanded_url'], cat)) 
                            print "inserted web"
                            flag = 1
                if flag == 0 and r['expanded_url'] != '':
                    #print w
                    if "%facebook.com%" in r['url'] or "%facebook.com%" in r['expanded_url'] or "%instagram%" in r['url'] or "%instagram%" in r['expanded_url']:
                        continue
                    else:
                        if w[web][0] == r['expanded_url']:
                            accmatch = accmatch + 1
                            cat = "Website + Lookup + numeric + so"
                            cursor.execute("insert into WholeLookupSoNume(id, screen_name, name, created_at, url, followers_count, friends_count, statuses_count, favourites_count, listed_count, contributors_enabled, profile_image_url, description, protected, location, lang, expanded_url, category) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(r['id'], r['screen_name'], r['name'], r['created_at'], r['url'], r['followers_count'], r['friends_count'], r['statuses_count'], r['favourites_count'], r['listed_count'], r['contributors_enabled'], r['profile_image_url'], r['description'], r['protected'], r['location'], r['lang'], r['expanded_url'], cat)) 
                            print "inserted web"
                            flag = 1
                
        if flag == 0:
            #print "image started"
            if r['profile_image_url'] != '':
                for j in range(0,len(w)):
                    if w[j][2] != None:
                        img1 = r['profile_image_url']
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
                            cat = "Image + Lookup + Nume + so"
                            if similarity >= 80 and similarity <90 and len(w[j][1]) >= 7:
                                accmatch = accmatch + 1
                                cursor.execute("insert into WholeLookupSoNume(id, screen_name, name, created_at, url, followers_count, friends_count, statuses_count, favourites_count, listed_count, contributors_enabled, profile_image_url, description, protected, location, lang, expanded_url, category) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(r['id'], r['screen_name'], r['name'], r['created_at'], r['url'], r['followers_count'], r['friends_count'], r['statuses_count'], r['favourites_count'], r['listed_count'], r['contributors_enabled'], r['profile_image_url'], r['description'], r['protected'], r['location'], r['lang'], r['expanded_url'], cat)) 
                                print "inserted image 1"
                                flag = 1
                            elif similarity >=90 and similarity <98 and len(w[j][1]) >= 5:
                                accmatch = accmatch + 1
                                cursor.execute("insert into WholeLookupSoNume(id, screen_name, name, created_at, url, followers_count, friends_count, statuses_count, favourites_count, listed_count, contributors_enabled, profile_image_url, description, protected, location, lang, expanded_url, category) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(r['id'], r['screen_name'], r['name'], r['created_at'], r['url'], r['followers_count'], r['friends_count'], r['statuses_count'], r['favourites_count'], r['listed_count'], r['contributors_enabled'], r['profile_image_url'], r['description'], r['protected'], r['location'], r['lang'], r['expanded_url'], cat)) 
                                print "inserted image 2"
                                flag = 1
                            elif similarity >= 97:
                                accmatch = accmatch + 1
                                cursor.execute("insert into WholeLookupSoNume(id, screen_name, name, created_at, url, followers_count, friends_count, statuses_count, favourites_count, listed_count, contributors_enabled, profile_image_url, description, protected, location, lang, expanded_url, category) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(r['id'], r['screen_name'], r['name'], r['created_at'], r['url'], r['followers_count'], r['friends_count'], r['statuses_count'], r['favourites_count'], r['listed_count'], r['contributors_enabled'], r['profile_image_url'], r['description'], r['protected'], r['location'], r['lang'], r['expanded_url'], cat)) 
                                print "inserted image 3"
                                flag = 1
                    else:
                        continue
            else:
                continue
    database.commit()
    print "found : ",found
    print "matched : ",accmatch
    
cursor.close()
database.commit()
database.close()

