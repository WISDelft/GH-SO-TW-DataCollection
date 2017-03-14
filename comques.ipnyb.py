
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
qa = []
p = []
for i in range(1,100):
    cursor.execute("select count(commentcount) from questions where commentcount = %s",[i])
    row = cursor.fetchone()
    print i," : ",row
    qa.append(row)
    if i < 11:
        p.append(row)
#   print "list", qa[i]
#print "it was here"
#plt.ion()
#print "here also"
x = [1,2,3,4,5,6,7,8,9,10]
#plt.plot(x,qa)
p1 = np.array(p)
arr = np.array(qa)
xt = np.array(x)
print np.mean(arr)
print np.std(arr)
print np.median(arr)

plt.figure()
plt.bar(xt, p1, align='center', alpha=0.1)
plt.xlabel('Number of comments')
plt.ylabel('Number of questions')
plt.title('Comments to a question')
plt.draw()
#plt.show()


cursor.close()

