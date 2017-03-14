import psycopg2
import csv

csv_data = csv.reader(file('/data/aditi/raw_data/github/projects.csv','rU'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
count = 0
x = 0
y = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_projects"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_projects(id integer PRIMARY KEY NOT NULL, url character varying(500), owner_id integer, name character varying(500) COLLATE pg_catalog."default" NOT NULL, description character varying(500), language character varying(500), created_at timestamp without time zone NOT NULL, forked_from integer, deleted smallint NOT NULL, updated_at timestamp without time zone)""")

print "Table created successfully"

for row in csv_data:
    try:
        for column in row:
            count = count + 1;
            #print "now : ",count
        if count == 10:
            #print "here : ",count
            for i,column in enumerate(row):
                if column == '\N':
                    row[i] = 0
                if row[9] == '0000-00-00 00:00:00':
                    #print "date changed"
                    row[9] = '1111-11-11 11:11:11'
                if row[6] == ',\N,2014-07-25 01:02:17"':
                    row[6] = '2014-07-25 01:02:17'
                if row[6] ==  ',\N,2014-09-07 03:15:19"':
                    row[6] = '2014-09-07 03:15:19'
                if row[6] == ',\\N,2014-11-17 17:04:44"':
                    row[6] = '2014-11-17 17:04:44'
                if row[6] ==  ',\N,2014-11-15 09:09:38"':
                    row[6] = '2014-11-15 09:09:38'
                if row[6] == ',\N,2014-11-17 08:56:45"':
                    row[6] = '2014-11-17 08:56:45'
                if row[6] == ',\N,2015-01-07 18:51:28"':
                    row[6] = '2015-01-07 18:51:28'
                if row[6] == ',\N,2015-12-18 22:59:31"':
                    row[6] = '2015-12-18 22:59:31'
                if row[6] == ',\N,2016-03-24 20:36:18"':
                    row[6] = '2016-03-24 20:36:18'
                if row[6] == ',\N,2016-04-24 03:03:22"':
                    row[6] = '2016-04-24 03:03:22'
                if row[6] == ',\N,2016-04-28 07:58:53"':
                    row[6] = '2016-04-28 07:58:53'
                if row[6] == ',\N,2016-08-25 01:07:42"':
                    row[6]= '2016-08-25 01:07:42'
                else:
                    if row[6] == '",\N,2016*"':
                        continue
                        print "this one did work!"


                if any(f.startswith(',\N,') and f.endswith('"') for f in row[6]):
                    continue
                    print "another resolved"

                #print row
            cursor.execute("INSERT INTO ghtorrent_projects (id, url, owner_id, name, description, language, created_at, forked_from, deleted, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)
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