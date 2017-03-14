
# coding: utf-8

# In[8]:

get_ipython().magic(u'matplotlib inline')
import numpy as np
import psycopg2
import matplotlib
#matplotlib.use('Agg')
import matplotlib.pyplot as plt

database = psycopg2.connect(database='netdata', user='postgres', password='postgres')
cursor = database.cursor()
qa = []
for i in range(1,80):
    cursor.execute("select count(answercount) from questions where answercount = %s",[i])
    row = cursor.fetchone()
    print i," : ",row
    qa.append(row)
#   print "list", qa[i]
#print "it was here"
#plt.ion()
#print "here also"
x = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
#plt.plot(x,qa)

arr = np.array(qa)
xt = np.array(x)
print np.mean(arr)
print np.std(arr)
print np.median(arr)
print arr
print xt

plt.figure()
plt.bar(xt, arr, align='center', alpha=0.1)
plt.xlabel('Number of answers')
plt.ylabel('Number of users')
plt.title('Answerers responding to a question')
plt.draw()
#plt.show()


cursor.close()

