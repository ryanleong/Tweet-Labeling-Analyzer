#!/usr/bin/env python
# encoding: utf-8
# Code adapted from https://gist.github.com/yanofsky/5436496

import couchdb
import json

#Database Name
database_name = "twitter_data"

def checkAndRemove(tweet):

    #connect to db
    couch = couchdb.Server('http://127.0.0.1:5984')
    db = couch[database_name]
    map_fun = '''function(doc) { emit(doc.tweet_id,doc); }'''
    
    #get all duplicates
    results = db.query(map_fun,key=tweet['tweet_id'])
    
    # remove only if returns more than 1
    if len(results) > 1:
        isSelf = True
        print "Found ducplicate. ID: ", tweet['tweet_id']
        
        for result in results:
            
            # skip one results to keep one
            if isSelf :
                isSelf = False
            else :
                # load json as array
                temp = json.loads(json.dumps(result.value))
                
                # delete entry from db
                db.delete(temp)
                print "Deleted tweet ID: ", tweet['tweet_id']
        
        # write log
        entry = str(len(results) - 1) + " duplicates found and deleted for tweet " + str(tweet['tweet_id']) + ". Expert Rating:" + str(tweet['expert_rating']) + ".\n"
        with open("checkDuplicateLog.txt", "a") as myfile:
            myfile.write(entry)


# main function
if __name__ == '__main__':

    print "Duplicate checker started.."

    couch = couchdb.Server('http://127.0.0.1:5984')
    db = couch[database_name]
    map_fun = '''function(doc) {
        emit(doc.id,doc); }'''

    results = db.query(map_fun)
    print "Number of tweets: ", len(results)
    count = 1;

    for tweet in results:
        # load json as array
        temp = json.loads(json.dumps(tweet.value))

        # check and remove duplicates
        checkAndRemove(temp);

        count = count + 1

