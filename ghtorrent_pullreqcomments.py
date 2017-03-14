
import psycopg2
import csv

csv_data = csv.reader(file('/data/aditi/raw_data/github/pull_request_comments.csv','rU'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
count = 0
x = 0
y = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_pull_request_comments"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_pull_request_comments(pull_req_id integer NOT NULL, user_id integer NOT NULL, comment_id integer, position integer, body character varying(500), commit_id integer NOT NULL, created_at timestamp without time zone NOT NULL)""")

print "Table created successfully"

for row in csv_data:
    try:
        for column in row:
            count = count + 1;
            #print "now : ",count
        if count == 7:
            #print "here : ",count
            for i,column in enumerate(row):
                if column == '\N':
                    row[i] = 0
                if row[6] == '0000-00-00 00:00:00':
                    print "date changed"
                    row[6] = '1111-11-11 11:11:11'
               

                #if any(f.startswith(',\N,') and f.endswith('"') for f in row[6]):
                    #continue
                    #print "another resolved"

                #print row
            cursor.execute("INSERT INTO ghtorrent_pull_request_comments (pull_req_id, user_id, comment_id, position, body, commit_id, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",row)
            print x," : ",count
            x = x + 1
            count = 0
        else:
            count = 0
    except (psycopg2.DataError, IndexError, psycopg2.InternalError) as e:
        print row," : ",e
        y = y + 1
        pass
print y
print x
cursor.close()
database.commit()
database.close()

print "CSV users data imported"