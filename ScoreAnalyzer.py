#!/usr/bin/env python
# encoding: utf-8
import couchdb
import json

# list of workers
listOfWorkers = []

# list of tweet
results = None

# calculate average of score for each tweet
def firstPass(db):
    
    for tweet in results:
        tweet = json.loads(json.dumps(tweet.value))
        
        totalRating = 0
        
        # calculate average
        for rating in tweet['worker_ratings']:
            totalRating += rating['rating']

        # save to tweet doc
        tweet['first_pass'] = (float(totalRating) / len(tweet['worker_ratings']))
        

    # update worker with weights
    updateWorkerData()
        

# next passes
def nextPass(db):
    for tweet in results:
        tweet = json.loads(json.dumps(tweet.value))
        
        totalRating = 0
        numOfParts = 0
        
        # go through worker ratings
        for rating in tweet['worker_ratings']:
            
            # find worker in listOfWorkers
            for worker in listOfWorkers:
                
                if worker['id'] == rating['worker_id']:
                    
                    # place weight on rating based on worker confidence
                    totalRating += rating['rating'] * worker['worker_confidence']
                    
                    # number of parts
                    numOfParts += worker['worker_confidence']
                    
                    break
                
        # calculate new average based on weights
        tweet['next_pass'] = totalRating / numOfParts
        
    # update worker with weights
    updateWorkerData()
                

def writeToDB(db):
    # write workers to DB
    for workerData in listOfWorkers:
        map_fun = '''function(doc) { if(doc.doc_type == "worker") {
        emit(doc.worker_id,doc); }}'''
        
        # query to get worker
        results2 = db.query(map_fun,key=workerData['id'])
        
        # initialize worker object
        worker = None
        
        # update worker in DB
        if len(results2) > 0:
            worker = results2.rows[0]
            worker = json.loads(json.dumps(worker.value))
            
            worker['worker_confidence'] = workerData['worker_confidence']
            worker['doc_type'] = "worker"
            
        # create worker
        else:
            worker = {'worker_id': workerData['id'], 'worker_confidence': workerData['worker_confidence']}
        
        # update db
        db.save(worker)
    
    # write tweets to DB
    for tweet in results:
        tweet = json.loads(json.dumps(tweet.value))
        
        # save score to db
        db.save(tweet)

def updateWorkerData():
    
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

    # calculate worker rating
    for workerData in listOfWorkers:
        # store the inverse of average difference as rating
        workerData['worker_confidence'] = 1/(float(workerData['total_diff']) / workerData['total_num'])


if __name__ == "__main__":
    
    print "Analyzing score.."
    
    couch = couchdb.Server('http://127.0.0.1:5984')
    db = couch['test_tweet']
    map_fun = '''function(doc) {
        emit(doc.doc_type,doc); }'''

    results = db.query(map_fun,key="tweet")
    
    # execute first pass
    firstPass(db)
    
    # next pass
    nextPass(db)
    
    # write results to DB
    writeToDB(db)

    print "Score analyzed."
    