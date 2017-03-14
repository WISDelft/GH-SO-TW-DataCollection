import psycopg2
import csv

csv_data = csv.reader(file('/data/aditi/raw_data/github/users.csv'))

database = psycopg2.connect(database='netdata', user='postgres', password='postgres') 
count = 0
x = 0
y = 0
cursor = database.cursor()
delete = "Drop table if exists ghtorrent_user"
print (delete)

mydata = cursor.execute(delete)

cursor.execute("""CREATE TABLE ghtorrent_user(id integer PRIMARY KEY NOT NULL, login character varying(500) COLLATE pg_catalog."default" NOT NULL, company character varying(500) COLLATE pg_catalog."default", created_at timestamp without time zone NOT NULL, type character varying(500) COLLATE pg_catalog."default" NOT NULL, fake smallint NOT NULL, deleted smallint NOT NULL, long numeric(11,8), lat numeric(10,8), country_code character(3) COLLATE pg_catalog."default", state character varying(500) COLLATE pg_catalog."default", city character varying(500) COLLATE pg_catalog."default", location character varying(1000) COLLATE pg_catalog."default")""")

print "Table created successfully"


for row in csv_data:
    try:
        for column in row:
            count = count + 1;
        if count == 13:
            for i,column in enumerate(row):
                if column == '\N':
                    row[i] = 0
        #cursor.execute("COPY ghtorrent_user (id, login, company, created_at, type, fake, deleted, long, lat, country_code, state, city, location) FROM 'row' DELIMITER ',' CSV NULL AS '\N'")
            cursor.execute("INSERT INTO ghtorrent_user (id, login, company, created_at, type, fake, deleted, long, lat, country_code, state, city, location) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row)
            print x," : ",count
            x = x + 1
            count = 0
        else:
            count = 0
    except psycopg2.DataError as e:
        print row," : ",e
        #pass
        #y = y + 1
#print y    
cursor.close()
database.commit()
database.close()

print "CSV users data imported"