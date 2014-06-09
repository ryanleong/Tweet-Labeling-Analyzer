#!/usr/bin/env python
# encoding: utf-8

import couchdb
import json
import sys
import math
import random

# Settings
databaseIP = 'http://localhost:5984'
database = ''
debug = False
useScoreFinder = True

# get command line arguements
if len(sys.argv) != 2:
    print 'python WorkerScoring.py <database_ip>'
    exit()
else:
    # Database name
    databaseIP = str(sys.argv[1])


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

        # offset the len of value as additional field is added on first pass
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


        # offset the len of value as additional field is added on first pass
        offset = 0
        if 'bias' in value:
            offset += 1
        if 'expertise' in value:
            offset += 1

        value['expertise'] = 1 - (total / (len(value) - offset)) ** 1


def aggregateResult():

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
        if tweetDoc['average'] == None:
            print tweetDoc['tweet_id'], " has no average."
        db.save(tweetDoc)

#######################################################
def getCorrelation(allData):

    n = 1500;
    totalx = 0
    totaly = 0
    totalMul = 0
    totalXSqSum = 0
    totalYSqSum = 0

    for data in allData:
        totalx += data[1]
        totaly += data[2]
        totalMul += data[1] * data[2]

        totalXSqSum += (data[1] * data[1])
        totalYSqSum += (data[2] * data[2])

    top = (n * totalMul) - (totalx * totaly)

    bottom = math.sqrt(((n * totalXSqSum) - (totalx * totalx)) * ((n * totalYSqSum) - (totaly * totaly)))

    print database, "Correlation: ", (top / bottom)

def convertScale(score):

    if score < 2.5:
        return 0
    elif score >= 2.5 and score < 3.5:
        return 1
    else:
        return 2

#######################################################

def calculateAvg():

    map_fun = '''function(doc) {
        emit(doc.doc_type,doc); }'''

    # Get tweets
    results = db.query(map_fun,key="tweet")

    # Calculate initial average
    firstAverage(results)

    if useScoreFinder == False:
        return

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

        if debug:
            # print Sum of squared difference at each pass
            print "%d. Difference: %f" % (count, (sumSquare - temp))


        if abs(sumSquare - temp) < 0.001:
            break

        sumSquare = temp



def calculateCorrelation():

    allData = []

    for tweetKey, tweetDoc in tweetDict.iteritems():
        allData.append((tweetDoc['tweet_id'], tweetDoc['expert_rating'], convertScale(tweetDoc['average'])))

    # correlation formula
    getCorrelation(allData)

def exportRatingGraphData():
    offset = 0.5
    filename = 'graphs/' + database + '_rating_graph.csv'

    f = open(filename,'w')
    f.write('expert,worker\n')

    for tweetKey, tweetDoc in tweetDict.iteritems():

        try: 
            # Convert to 3 point scale
            workerLabel = convertScale(tweetDoc["average"])

            expert = 0.0

            expert = random.uniform((float(tweetDoc['expert_rating']) - offset), (float(tweetDoc['expert_rating']) + offset))
            workerLabel = random.uniform((workerLabel - offset), (workerLabel + offset))

            # # add small random offset for graph plotting
            # if tweetDoc['expert_rating'] != 0:
            #     expert = random.uniform((float(tweetDoc['expert_rating']) - offset), (float(tweetDoc['expert_rating']) + offset))
            # elif tweetDoc['expert_rating'] == 2:
            #     expert = random.uniform((float(tweetDoc['expert_rating']) - offset), float(tweetDoc['expert_rating']))
            # else:
            #     expert = random.uniform(float(tweetDoc['expert_rating']), (float(tweetDoc['expert_rating']) + offset))
                
            # if workerLabel != 0:
            #     workerLabel = random.uniform((workerLabel - offset), (workerLabel + offset))
            # elif tweetDoc['expert_rating'] == 2:
            #     workerLabel = random.uniform((workerLabel - offset), workerLabel)
            # else:
            #     workerLabel = random.uniform(workerLabel, (workerLabel + offset))

            output = "%f,%f\n" % (expert, workerLabel)
            f.write(output)
        except:
            continue

    f.close()

def exportWorkerExpertiseGraph():
    filename = 'graphs/' + database + '_worker_expertise_graph.csv'

    f = open(filename,'w')
    f.write('workerID,expertise\n')

    for key, value in workersDict.iteritems():
        try:
            output = "%s,%s\n" % (key, value['expertise'])
            f.write(output)
        except:
            continue

    f.close()

if __name__ == "__main__":
    # set up connection to db server
    couch = couchdb.Server(databaseIP)

    print "Calculating NBN Worker to Expert Correlation.."

    database = 'nbn_tweets'

    try:
        db = couch[database]
    except:
        print 'No such database.'

    calculateAvg()
    calculateCorrelation()
    exportRatingGraphData()
    exportWorkerExpertiseGraph()

    ################################################

    print "Calculating Flu Worker to Expert Correlation.."

    database = 'flu_tweets'
    workersDict = {}
    tweetDict = {}

    try:
        db = couch[database]
    except:
        print 'No such database.'

    calculateAvg()
    calculateCorrelation()
    exportRatingGraphData()
    exportWorkerExpertiseGraph()
