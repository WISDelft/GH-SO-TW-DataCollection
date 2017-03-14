import psycopg2
import csv

csv_data = csv.reader(file('/data/aditi/raw_data/github/project_members.csv','rU'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
count = 0
x = 0
y = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_project_members"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_project_members(repo_id integer NOT NULL, user_id integer NOT NULL, created_at timestamp without time zone NOT NULL, ext_ref_id character varying(24) COLLATE pg_catalog."default" NOT NULL)""")

print "Table created successfully"

for row in csv_data:
    try:
        for column in row:
            count = count + 1;
            #print "now : ",count
        if count == 4:
            #print "here : ",count
            for i,column in enumerate(row):
                if column == '\N':
                    row[i] = 0
             

                #print row
            cursor.execute("INSERT INTO ghtorrent_project_members (repo_id, user_id, created_at, ext_ref_id) VALUES (%s,%s,%s,%s)",row)
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