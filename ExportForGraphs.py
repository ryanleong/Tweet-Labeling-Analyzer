#!/usr/bin/env python
import couchdb
import json
import sys

# Settings
database = ''
databaseIP = 'http://localhost:5984'

# DB instance
db = None

# get command line arguements
if len(sys.argv) != 2:
    print 'python WorkerForGraphs.py <database_ip>'
    exit()
else:
    # Database name
    databaseIP = str(sys.argv[1])


def convertScale(score):

    if score < 2.5:
        return 0
    elif score >= 2.5 and score < 3.5:
        return 1
    else:
        return 2

def export():
    
    map_fun = '''function(doc) {
    emit(doc.tweet_id,{"expert" : doc.expert_rating, "worker": doc.average}); }'''
    
    results = db.query(map_fun)

    filename = 'graphs/' + database + '_graph.csv'

    f = open(filename,'w')
    f.write('expert,worker\n')
    
    for tweet in results:

        try: 
            # Convert to 3 point scale
            workerLabel = convertScale(tweet.value["worker"])

            output = "%s,%d\n" % (tweet.value['expert'], workerLabel)
            f.write(output)
        except:
            continue

    f.close()

if __name__ == "__main__":
    
    # database IP    
    couch = couchdb.Server(databaseIP)

    print 'Exporting NBN data...'
    database = 'nbn_tweets'

    try:
        # set database to query
        db = couch[database]
        export()
    except:
        print 'No such database.'

    print 'Done!'

    print 'Exporting Flu data...'
    database = 'flu_tweets'

    try:
        # set database to query
        db = couch[database]

    except:
        print 'No such database.'
    export()

    print 'Done!'
