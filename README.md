Tweet Labeling Analyzer
=======================

A Tweet Labeling Analyzer to compare the labeling done by workers on Amazon Mechanical Turk (AMT) and those done by an Expert.
This analyzer is used in the experiment for testing the effectiveness of AMT in the field of Computer Science Research.
This repository includes:

	ImporttoDB.py :		Imports twitter data from a CSV file, separated by whitespace in the format (<expertRating>, <tweetID>, <tweet>). Data will then be inserted into a CouchDB database.
	
	ExporttoAMTSets :	Exports tweets to a CSV file, in sets of 50, to be uploaded to AMT.
