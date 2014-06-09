Tweet Labeling Analyzer
=======================

A Tweet Labeling Analyzer to compare the labeling done by workers on Amazon Mechanical Turk (AMT) and those done by an Expert.
This analyzer is used in the experiment for testing the effectiveness of AMT in the field of Computer Science Research.
This repository includes:

	ImporttoDB.py :		Imports twitter data from a CSV file, separated by whitespace in the format 
						(<expertRating>, <tweetID>, <tweet>). Data will then be inserted into a 
						CouchDB database.
	
	ExporttoAMTSets :	Exports tweets to a CSV file, in sets of 50, to be uploaded to AMT.

	ImportFromAMT :		Imports result data from AMT results CSV into database.
	
	Analyzer :			Calculates aggregate worker scores and does correlation analysis. Also exports data for graphs.

	form.html :			Holds the form layout of the Amazon Mechanical Turk Survey used in the HITs.

Prior to running the scripts, CouchDB needs to be installed on the machine. The Python-CouchDB package is required to 
run the scripts, which can be installed by running "sudo apt-get install python-couchdb". Take note that the scripts 
have to be run sequentially, in the above order.

Experts HTML form is hosted on an Apache Tomcat server. This requires it to be installed prior to running, in addtion to 
running the "ImporttoDB.py" state above. The files are in the AMT directory, which is the source code for the Expert 
labeling forms. This includes:

	nbn.jsp : 			Page with form for Experts to label NBN Tweets.

	flu.jsp : 			Page with form for Experts to label Flu Tweets.

	Store.java :		Inserts gathered NBN labels into db.

	StoreFlu.java :		Inserts gathered Flu labels into db.

	form.war :			The Apache Tomcat server package that is to be deployed.