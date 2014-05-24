#!/usr/bin/env python
import couchdb
import json
import sys

# Settings
database = ''
databaseIP = 'http://localhost:5984'

# get command line arguements
if len(sys.argv) != 2:
    print 'python WorkerForGraphs.py <database_name>'
    exit()
else:
    # Database name
    database = str(sys.argv[1])


def convertScale(score):

    if score < 2.5:
        return 0
    elif score >= 2.5 and score < 3.5:
        return 1
    else:
        return 2

if __name__ == "__main__":
    
    # database IP    
    couch = couchdb.Server(databaseIP)
    
    # set database to query
    db = couch[database]
    
    map_fun = '''function(doc) {
    emit(doc.tweet_id,{"expert" : doc.expert_rating, "worker": doc.average}); }'''
    
    results = db.query(map_fun)

    f = open('graph.csv','w')
    f.write('expert,worker\n')
    
    for tweet in results:
        # Convert to 3 point scale
        workerLabel = convertScale(tweet.value["worker"])

        output = "%s,%d\n" % (tweet.value['expert'], workerLabel)
        f.write(output)

    f.close()
