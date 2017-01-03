from elasticsearch import Elasticsearch
import xml.etree.ElementTree as ET
import urllib2
import time
#import mysql.connector as mariadb
import MySQLdb
import datetime

db = MySQLdb.connect("127.0.0.1","root","password","Xdist")
cursor = db.cursor()
es = Elasticsearch(['172.16.1.18'])
port=9200

# Do not delete the below line, this might be needed incase of MariaDB
#mariadb_connection = mariadb.connect(host='172.16.1.206', port='3306', user='suse', password='password', database='Xdist')
#cursor = mariadb_connection.cursor()

lt = time.strftime('%Y-%m-%d %H:%M:%S')

utc_datetime = datetime.datetime.utcnow()
ltime =  utc_datetime.strftime('%Y-%m-%dT%H:%M:%S')

#ltime = time.strftime('%Y-%m-%dT%H:%M:%S')

xdistserver = [
"172.16.1.63",
"172.16.1.64",
"172.16.1.65",
"172.16.1.66",
"172.16.1.67",
"172.16.1.68",
"172.16.1.208",
"172.16.1.209",
"172.16.1.210",
"172.16.1.211",
"172.16.1.212",
"172.16.1.213",
]

xdistairline = [
"SGAPINavitaire",
"1AWS4",
"6EAPI",
"G8CP",
"GALDOM",
"1AWSINT4"
]

xdistkpithread = {
'SpiceJet':{'pgcode':"SGAPINavitaire", 'created':0, 'active':0},
'Indigo':{'pgcode':"6EAPINavi", 'created':0, 'active':0}
}


#es.index(index='xdist', doc_type='post', body={
#  'supplier': 'spicejet',
#  'date': '2016-10-23',
#  'threadcount': '5'
#})



xcount = len(xdistairline)

for g in xdistserver:
    url = "http://{}:9090/services/statsxml.jsp".format(g)
    try:
    	hxml = urllib2.urlopen(url)
    except urllib2.URLError, e:
	continue
    tree = ET.parse(hxml)
    root = tree.getroot()
    for child in root:
        a = child.attrib
    for i in child:
        for d in range(xcount):
            if i.attrib['name'] == xdistairline[d]:
                for z in i:
                    try:
                        if z.attrib['name'] in "ActiveThreads":
                            global ActiveThreads
                            global CreatedThreads
			    global DroppedRequests
			    global MaxPendingQueueSize
                            CT = z.attrib["value"]
                            ActiveThreads = CT
                        if z.attrib['name'] in "CreatedThreads":
                            CT = z.attrib["value"]
                            CreatedThreads = CT
			if z.attrib['name'] in "DroppedRequests":
			    CT = z.attrib["value"]
			    DroppedRequests = CT
			if z.attrib['name'] in "MaxPendingQueueSize":
                            CT = z.attrib["value"]
                            MaxPendingQueueSize = CT
                    except KeyError:
                        continue
                print "{} Active Threads are {} and Created Threads are {} on server {}".format(xdistairline[d], ActiveThreads, CreatedThreads, g)
                print "Date is {}".format(lt)
		NumAT = int(ActiveThreads)
		es.index(index='xdist', doc_type='threads', body={
  			'supplier': xdistairline[d],
  			'date': ltime,
  			'threadcount': NumAT
		})


                #sql = """INSERT INTO Threads(Date, Supplier, ActiveThreads, ThreadsCreated, IPAddress)
                 #        VALUES ('lt', 'xdistairline[d]', ActiveThreads, 'CreatedThreads', 'g')"""
                try:
#                    sql = """INSERT INTO Threads(Date, Supplier, ActiveThreads, ThreadsCreated, IPAddress)
#                             VALUES (%s,%s,%s,%s,%s), ('lt', 'xdistairline[d]', 'ActiveThreads', 'CreatedThreads', 'g')"""
                    cursor.execute("INSERT INTO Threads (Date,Supplier,ActiveThreads,ThreadsCreated,IPAddress,DroppedRequests,MaxPendingQueueSize) VALUES (%s,%s,%s,%s,%s,%s,%s)", (lt, xdistairline[d], ActiveThreads, CreatedThreads, g, DroppedRequests, MaxPendingQueueSize))
                    #cursor.execute(sql)
                except mariadb.Error as error:
                    print("Error: {}".format(error))
                db.commit()
db.close()
print ltime
