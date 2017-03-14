import psycopg2
import csv

#csv_data = csv.reader(file('/data/aditi/raw_data/github/followers.csv'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
#count = 0
#x = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_follower"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_follower(follower_id integer NOT NULL, user_id integer NOT NULL, created_at timestamp without time zone NOT NULL)""")

print "Table created successfully"

#try:
#for row in csv_data:
    #for column in row:
        #count = count + 1;
    #if count == 13:
cursor.execute("COPY ghtorrent_follower (follower_id, user_id, created_at) FROM '/data/aditi/raw_data/github/followers.csv' DELIMITER ',' CSV NULL AS '\N'")
        #cursor.execute("INSERT INTO ghtorrent_user (id, login, company, created_at, type, fake, deleted, long, lat, country_code, state, city, location) VALUES NULL '\N'(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)
        #x = x + 1
        #count = 0
    #else:
        #print count
        #print row
        #count = 0

#except: 
#EXEC SQL WHENEVER SQLERROR CONTINUE;
#print x
cursor.close()
database.commit()
database.close()

print "CSV users data imported"

  #cursor.execute("INSERT INTO ghtorrent_user (id, login, company, created_at, type, fake, deleted, long, lat, country_code, state, city, location) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) NULL AS '\N'",row)
  