
# coding: utf-8

# In[3]:

get_ipython().magic(u'matplotlib inline')
import numpy as np
import psycopg2
import matplotlib
#matplotlib.use('Agg')
import matplotlib.pyplot as plt

database = psycopg2.connect(database='netdata', user='postgres', password='postgres')
cursor = database.cursor()
votes = []
a = []
b = []
#print "hh"
cursor.execute("select distinct user_id from ghtorrent_orgmembers")
v = cursor.fetchall()
print "asdf"
for i in range(1,len(v)):
    #print "yy"
    print len(v)
    #print v[1]
    cursor.execute("select count(org_id) from ghtorrent_orgmembers where user_id = %s",v[i])
    row = cursor.fetchone()
    print i," : ",row
    votes.append(row)
    if (i<16):
        a.append(row)
        b.append(v[i])
#   print "list", qa[i]
#print "it was here"
#plt.ion()
#print "here also"
#x = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
#plt.plot(x,qa)
a1 = np.array(a)
b1 = np.array(b)
arr = np.array(votes)
#xt = np.array(x)
print np.mean(arr)
print np.std(arr)
print np.median(arr)

#print xt
print a1

plt.figure()
plt.bar(b1, a1, align='center',alpha=0.1)
plt.xlabel('Users')
plt.ylabel('Organisation ')
plt.title('Users to organisations')
plt.draw()
#plt.show()


cursor.close()

