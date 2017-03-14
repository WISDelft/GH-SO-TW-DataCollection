import psycopg2
import csv

csv_data = csv.reader(file('/data/aditi/raw_data/github/commits.csv','rU'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
count = 0
x = 0
y = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_commits"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_commits(id integer PRIMARY KEY NOT NULL, sha character varying(40), author_id integer, committer_id integer, project_id integer, created_at timestamp without time zone NOT NULL)""")

print "Table created successfully"

for row in csv_data:
    try:
        for column in row:
            count = count + 1;
            #print "now : ",count
        if count == 6:
            #print "here : ",count
            for i,column in enumerate(row):
                if column == '\N':
                    row[i] = 0
                if row[5] == '0000-00-00 00:00:00':
                    print "date changed"
                    row[5] = '1111-11-11 11:11:11'
               

                #if any(f.startswith(',\N,') and f.endswith('"') for f in row[6]):
                    #continue
                    #print "another resolved"

                #print row
            cursor.execute("INSERT INTO ghtorrent_commits (id, sha, author_id, committer_id, project_id, created_at) VALUES (%s,%s,%s,%s,%s,%s)",row)
            print x," : ",count
            x = x + 1
            count = 0
        else:
            count = 0
    except psycopg2.DataError as e:
        print row," : ",e
        #pass
        y = y + 1
print y
print x
cursor.close()
database.commit()
database.close()

print "CSV users data imported"