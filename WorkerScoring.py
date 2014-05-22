#!/usr/bin/env python
# encoding: utf-8

import couchdb
import json
import sys

# Settings
database = ''
databaseIP = 'http://localhost:5984'

# get command line arguements
if len(sys.argv) != 2:
    print 'python WorkerScoring.py <database_name>'
    exit()
else:
    # Database name
    database = str(sys.argv[1])


# DB instance
db = None

# Worker Dictionary
#workers = {"workerid" : {"tweet_id" : "tweet_rating"}}
workersDict = {}
tweetDict = {}

def firstAverage(results):

    count = 0

    for tweet in results:
        tweet = json.loads(json.dumps(tweet.value))

        totalRatings = 0

        # Get total ratings
        for rating in tweet['worker_ratings']:
            totalRatings += rating['rating']

            # store rating to workerDict
            if rating['worker_id'] in workersDict:
                # if worker is in worker dictionary
                workersDict[rating['worker_id']][tweet['tweet_id']] = rating['rating']
            else:
                # if worker is not in dictionary
                workersDict[rating['worker_id']] = { tweet['tweet_id'] : rating['rating'] }

        # calculate average
        tweet['average'] = float(totalRatings) / len(tweet['worker_ratings'])

        # Store tweet in tweet dictionary
        tweetDict[tweet['tweet_id']] = tweet

        #newDB.save(tweet)


# For workers
def removeBias():
    # print workersDict['AUBB8KU9XV2R8']

    for key, value in workersDict.iteritems():

        totalWorkerRatings = 0

        for resultKey, resultValue in tweetDict.iteritems():

            # Get rating if tweet is rated by current worker
            for rating in resultValue['worker_ratings']:

                if rating['worker_id'] == key:
                    totalWorkerRatings += (rating['rating'] - resultValue['average'])

        offset = 0
        if 'bias' in value:
            offset += 1
        if 'expertise' in value:
            offset += 1

        value['bias'] = totalWorkerRatings / (len(value) - offset)


def adjustRatings():

    for resultKey, resultValue in tweetDict.iteritems():

        for rating in resultValue['worker_ratings']:
            rating['rating'] = rating['rating'] - workersDict[rating['worker_id']]['bias']


def calculateWorkerExpertise():
    for key, value in workersDict.iteritems():

    	total = 0

    	# go through list of tweets
        for resultKey, resultValue in tweetDict.iteritems():

            # Get average if tweet is rated by current worker
            for rating in resultValue['worker_ratings']:

                if rating['worker_id'] == key:
                    total = abs(resultValue['average'] - rating['rating'])


        offset = 0
        if 'bias' in value:
            offset += 1
        if 'expertise' in value:
            offset += 1

        value['expertise'] = 1 - (total / (len(value) - offset)) ** 6
        # print value['expertise'], "\n"


def aggregateResult():
    # WorkerExpertise * currAvgScore * 1 or 0
    # WorkerExpertise * 1 or 0

    for tweetKey, tweetDoc in tweetDict.iteritems():
        
        numerator = 0
        denominator = 0

        # go through ratings of current tweet
        for rating in tweetDoc['worker_ratings']:

            # get total of (expertise * average score)
            numerator += workersDict[rating['worker_id']]['expertise'] * rating['rating']

            # get total of (expertise)
            denominator += workersDict[rating['worker_id']]['expertise']

        # calculate new average
        tweetDoc['average'] = (numerator/denominator)


def sumOfSquared():

    totalSquare = 0
    total = 0

    for tweetKey, tweetDoc in tweetDict.iteritems():
        totalSquare += tweetDoc['average'] ** 2

        total += tweetDoc['average']
    
    return totalSquare - (total/len(tweetDict))

def returnToDB():

    for tweetKey, tweetDoc in tweetDict.iteritems():

        # del tweetDoc['_rev']

        db.save(tweetDoc)

if __name__ == "__main__":

    # set up connection to db server
    couch = couchdb.Server(databaseIP)
    db = couch[database]

    print "Calculating tweet worker scores.."

    map_fun = '''function(doc) {
        emit(doc.doc_type,doc); }'''

    # Get tweets
    #results = db.iterview('_design/getall/_view/getall',50)
    results = db.query(map_fun,key="tweet")

    # Calculate initial average
    firstAverage(results)

    # calculate sum of squared
    sumSquare = sumOfSquared()

    count = 0
    while True:
        count +=1

        removeBias()

        adjustRatings()

        calculateWorkerExpertise()

        aggregateResult()

        # calculate final sum of squared
        temp = sumOfSquared()

        print "%d. Difference: %f" % (count, (sumSquare - temp))
        if abs(sumSquare - temp) < 0.001:
            break

        sumSquare = temp

    returnToDB()

