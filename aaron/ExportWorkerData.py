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


def export():
    
    map_fun = '''function(doc) {
    emit(doc.tweet_id,{"labels" : doc.worker_ratings, "tweet": doc.tweet_id}); }'''
    
    results = db.query(map_fun)

    filename = database + '_worker_labels.dat'

    f = open(filename,'w')
    #f.write('labels,worker\n')
    
    for tweet in results:

        try: 
            ratings = '{'

            for label in tweet.value['labels']:
                ratings += str(label['rating']) + ","

            ratings = ratings[:-1] + '}'

            output = "%s %s\n" % (ratings, tweet.value["tweet"])

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
