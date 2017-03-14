import psycopg2
import csv

csv_data = csv.reader(file('/data/aditi/raw_data/github/pull_requests.csv','rU'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
count = 0
x = 0
y = 0
z = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_pull_requests"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_pull_requests(id integer NOT NULL, head_repo_id integer, base_repo_id integer NOT NULL, head_commit_id integer, base_commit_id integer NOT NULL, pullreq_id integer NOT NULL, intra_branch smallint NOT NULL)""")

print "Table created successfully"

for row in csv_data:
    #print "one"
    for i,column in enumerate(row):
        if column == '\N':
            row[i] = 0
        if column == '\\':
            row[i] = 0
    try:
        z = z + 1
        #print row[1]," : two"
        if row[0] >= -10:
            #print "three : ",z
            for column in row:
                count = count + 1;
                #print "now : ",count
            #if count == 2:
            #print "here : ",count
                #for i,column in enumerate(row):
                    #if row[7] == '0000-00-00 00:00:00':
                        #print "date changed"
                        #row[7] = '1111-11-11 11:11:11'         
                #print "inserting"
            cursor.execute("INSERT INTO ghtorrent_pull_requests (id, head_repo_id, base_repo_id, head_commit_id, base_commit_id, pullreq_id, intra_branch) VALUES (%s,%s,%s,%s,%s,%s,%s)",row)
            print x," : ",count
            x = x + 1
            count = 0
            #else:
             #   count = 0
        else: 
            pass
    except (psycopg2.DataError, IndexError) as e:
        print row," : exception : ",e
        y = y + 1
        pass

print z
print y
print x
cursor.close()
database.commit()
database.close()

print "CSV users data imported"