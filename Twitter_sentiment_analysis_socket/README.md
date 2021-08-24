twitter-sentiment-analysis-using-socket


twitter_connection.py

This program consist code to create a socket on the port 9998 on localhost.

Using tweepy api the tweet's are getting fetched.

The API key's provided by twitter are used to create and establish the connection.


tweet_analysis.py

This program consist code to recieve the messages comming from the fist program.

The recieved messages is again passed for sentiment analysis.

for sentiment analysis using the textblob library whic gives polarity of the user.

after getting the polarity convering into a number and counting the number of positive and and negative tweet's.


plot_data.py

This program consist code to plot graph.

To plot the graph reading the input from csv file and using the line chart to plot the graph.