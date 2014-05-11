#!/usr/bin/env python
# encoding: utf-8
import couchdb
import json

# calculate average of score for each tweet
def firstPass(results, db):
    
    for tweet in results:
        tweet = json.loads(json.dumps(tweet.value))
        
        totalRating = 0
        
        # calculate average
        for rating in tweet['worker_ratings']:
            totalRating += rating['rating']

        # save to tweet doc
        tweet['first_pass'] = (float(totalRating) / len(tweet['worker_ratings']))
        
        # save score to db
        db.save(tweet)
        
    return results

def updateWorkerData(db, results):
    
    listOfWorkers = []
    
    # go through all tweets
    for tweet in results:
        tweet = json.loads(json.dumps(tweet.value))
        
        # go through tweet ratings
        for rating in tweet['worker_ratings']:
            
            inListOfWorkers = False
            
            # calculate difference
            difference = abs(rating['rating'] - tweet['first_pass'])
            
            # check if worker exist
            for worker in listOfWorkers:
                  
                if worker['id'] == rating['worker_id']:
                     
                    # update worker data
                    worker['total_diff'] += difference
                    worker['total_num'] += 1
                     
                    inListOfWorkers = True
                    break

            # add new worker
            if not inListOfWorkers:
                listOfWorkers.append({'id': rating['worker_id'], 'total_diff': difference, 'total_num': 1})
    
    # write to DB
    for workerData in listOfWorkers:
        map_fun = '''function(doc) { if(doc.doc_type == "worker") {
        emit(doc.worker_id,doc); }}'''
        
        # query to get worker
        results = db.query(map_fun,key=workerData['id'])
        
        # calculate worker rating
        workerRating = float(workerData['total_diff']) / workerData['total_num']
        
        # initialize worker object
        worker = None
        
        # update worker in DB
        if len(results) > 0:
            worker = results.rows[0]
            worker = json.loads(json.dumps(worker.value))
            
            worker['worker_rating'] = workerRating
            worker['doc_type'] = "worker"
            
        # create worker
        else:
            worker = {'worker_id': workerData['id'], 'worker_rating': workerRating}
        
        # update db
        db.save(worker)      


if __name__ == "__main__":
    
    print "Analyzing score.."
    
    couch = couchdb.Server('http://127.0.0.1:5984')
    db = couch['test_tweet']
    map_fun = '''function(doc) {
        emit(doc.doc_type,doc); }'''

    results = db.query(map_fun,key="tweet")
    
    # execute first pass
    results = firstPass(results, db)

    # update worker with weights
    updateWorkerData(db, results)
    
    print "Score analyzed."
    