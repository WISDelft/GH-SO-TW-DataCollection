
# coding: utf-8

# In[ ]:

get_ipython().magic(u'matplotlib inline')
import numpy as np
import psycopg2
import matplotlib
#matplotlib.use('Agg')
import matplotlib.pyplot as plt

database = psycopg2.connect(database='netdata', user='postgres', password='postgres')
cursor = database.cursor()
user = []
u = []
ques = []
q = []
cursor.execute("select distinct user_id from ghtorrent_follower")
rows = cursor.fetchall()

for i in range(1,len(rows)):
    print len(rows)
    cursor.execute("select count(follower_id) from ghtorrent_follower where user_id = %s",rows[i])
    r = cursor.fetchone()
    print i," : ",r
    ques.append(r)
    if i < 16:
        q.append(r)
        user.append(rows[i])
#   print "list", qa[i]
#print "it was here"
#plt.ion()
#print "here also"
#x = [1,2,3,4,5,6,7,8,9,10]
#plt.plot(x,qa)
p1 = np.array(q)
arr = np.array(ques)
xt = np.array(user)
print np.mean(arr)
print np.std(arr)
print np.median(arr)

plt.figure()
plt.bar(xt, p1, align='center', alpha=0.1)
plt.xlabel('Users')
plt.ylabel('Number of followers')
plt.title('Followers per user')
plt.draw()
#plt.show()


cursor.close()

