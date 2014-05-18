#!/usr/bin/env python
import couchdb

# Database name
database = 'final_tweets'

if __name__ == "__main__":
    
    # database IP    
    couch = couchdb.Server('http://127.0.0.1:5984')
    
    # set database to query
    db = couch[database]
    
    map_fun = '''function(doc) {
    emit(doc.tweet_id,null); }'''
    
    results = db.iterview('_design/getratings/_view/expert',2000)
    
    for tweet in results:
    	key = '%s%s' % (tweet.key, ",")
    	tweetStr = '%s%s' % (key, tweet.value)
        print tweetStr


    # results = db.iterview('_design/count/_view/numofagree',2000)
    
    # for tweet in results:
    # 	tweetStr = '%s%s' % ("Agree,", tweet.value)
    #     print tweetStr

    # results = db.iterview('_design/count/_view/numofdisagree',2000)
    
    # for tweet in results:
    # 	tweetStr = '%s%s' % ("Disagree,", tweet.value)
    #     print tweetStr

    # results = db.iterview('_design/count/_view/numofundefined',2000)
    
    # for tweet in results:
    # 	tweetStr = '%s%s' % ("Unsure,", tweet.value)
    #     print tweetStr