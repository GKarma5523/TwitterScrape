import json
import requests
import time
from data import TweetAnalysis, Tweet, User, Analysis, queries
import pyodbc
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient

def run():
  print('Starting script')
  startTime = time.time()
  twitterJsonList = fetchTwitter()
  tweets,users = formatTweets(twitterJsonList)
  analysisList = analyzeTweets(tweets)
  tweetAnalysisList = createTweetAnalysisList(tweets, users, analysisList)
  saveToDatabase(tweetAnalysisList)
  print('Finished script')
  print('Script finished in {} seconds'.format(time.time() - startTime))


def fetchTwitter():
  bearerToken = 'AAAAAAAAAAAAAAAAAAAAAFRoVgEAAAAAwavgzrgIz%2FMGci%2F56lqliXCSG68%3DLHxMkBcaPOneVEuexaD1CvQnrSj0pqJiRZpg6zMEoxTBcIN2aZ'
  searchUrl = "https://api.twitter.com/2/tweets/search/recent"
  query_params = {
      'max_results': 100, # TODO Change to 100
      'user.fields': "id,username,name,location",
      'expansions': "author_id",
      'tweet.fields': 'created_at,public_metrics,source'
  }
  headers = {
      'Authorization': 'Bearer {}'.format(bearerToken),
      'User-Agent': 'TopicPull-RReed'
  }

  nextToken = True
  tweetList = []
  count = 0

  for key, value in queries.items():
    print('Starting query {}'.format(key))
    query_params['query'] = value

    while nextToken and count < 10: # TODO Change to 10
      print('Making request {}'.format(count))
      res = requests.request("GET", searchUrl, headers=headers, params=query_params)

      if res.status_code == 429:
        print('Rate limit exceeded')
        print(res.headers['x-rate-limit-reset'])
        exit()

      resJson = res.json()
      resJson['topic'] = key
      tweetList.append(resJson)
      if 'meta' in resJson:
        if 'next_token' in resJson['meta']:
          nextToken = resJson['meta']['next_token']
          query_params['next_token'] = nextToken
      count += 1

    count = 0
    nextToken = True
    print('Finished query {}'.format(key))

  return tweetList


def formatTweets(tweetList):

  tweetObjects = []
  userObjects = []

  for page in tweetList:
    for tweet in page['data']:
        tweetObjects.append(Tweet(tweet['id'], tweet['text'], tweet['author_id'], tweet['public_metrics']['retweet_count'], tweet['public_metrics']['reply_count'], tweet['public_metrics']['like_count'], tweet['public_metrics']['quote_count'], tweet['created_at'], tweet['source'], tweet, page['topic']))

        for user in page['includes']['users']:
          if 'location' in user:
            userObjects.append(User(user['id'], user['username'], user['name'], user['location']))
          else:
            userObjects.append(User(user['id'], user['username'], user['name'], "None"))

  return tweetObjects, userObjects


def analyzeTweets(tweets):

  print('Starting analysis')

  apiEndpoint = "https://gkarma5523.cognitiveservices.azure.com/"
  apiKey = "bbb9fe98f92541dead3d2d7791297a49"
  apiCall = TextAnalyticsClient(endpoint=apiEndpoint, credential=AzureKeyCredential(apiKey))

  callList = []

  for tweet in tweets:
    callList.append({
        'id': str(tweet.id), 
        'language': 'en', 
        'text': tweet.text 
    })

  # Send request to Cognitive Services API
  results = []
  index = 0
  length = len(callList)

  while index < length:
    batch = callList[index:index+10]
    results += apiCall.analyze_sentiment(batch, show_opinion_mining=True)
    index += 10

  sentimentList = []
  keyPhraseList = []
  analysisList = []

  for sentiment in results:
      sentimentList.append(sentiment);

  keyPhrases = []
  index = 0

  while index < length:
    batch = callList[index:index+10]
    keyPhrases += apiCall.extract_key_phrases(batch)
    index += 10

  for keyPhrase in keyPhrases:
      keyPhraseList.append(keyPhrase)

  # Associate sentiments with keyphrases
  for sentiment in sentimentList:
    matchingKeyPhrase = list(filter(lambda x: x.id == sentiment.id, keyPhraseList))
    if matchingKeyPhrase:
      analysisList.append(Analysis(sentiment.id, matchingKeyPhrase[0].key_phrases, sentiment.sentiment, sentiment.confidence_scores.positive, sentiment.confidence_scores.neutral, sentiment.confidence_scores.negative))
  
  return analysisList
        
          
def createTweetAnalysisList(tweets, users, analysisList):
  
  tweetAnalysisList = []

  for tweet in tweets:
    matchingUser = list(filter(lambda x: x.id == tweet.authorId, users))
    matchingAnalysis = list(filter(lambda x: x.id == tweet.id, analysisList))
    if matchingUser:
      tweetAnalysisList.append(TweetAnalysis(tweet, matchingUser[0], matchingAnalysis[0]))

  return tweetAnalysisList


def saveToDatabase(tweetAnalysisList):

  print('Saving to database')

  server = 'wsu-cs3550.westus2.cloudapp.azure.com'
  database = 'garrettkuns'
  username = 'garrettkuns'
  password = 'duck.queens.ages'

  connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password) 

  cursor = connection.cursor()

  for tweet in tweetAnalysisList: # Add tweet topic and userID to tweet table
    # Insert User
    userID = cursor.execute("EXEC InsertUser @TwitterUserID = ?, @UserName = ?, @Name = ?, @Location = ?", tweet.user.id, tweet.user.username, tweet.user.name, tweet.user.location).fetchval()
    
    # Insert Source
    sourceID = cursor.execute("EXEC InsertSource @SourceType = ?", tweet.tweet.source).fetchval()

    # Insert Topic
    topicID = cursor.execute("EXEC InsertTopics @Topic = ?", tweet.tweet.topic).fetchval()

    # Insert Confidence Score
    confidenceScoreID = cursor.execute("EXEC InsertConfidenceScore @Positive = ?, @Neutral = ?, @Negative = ?", tweet.analysis.positive, tweet.analysis.neutral, tweet.analysis.negative).fetchval()

    # Insert Tweet
    tweetID = cursor.execute("EXEC InsertTweets @TwitterTweetID = ?, @TweetText = ?, @CreateTime = ?, @RetweetCount = ?, @ReplyCount = ?, @LikeCount = ?, @QuoteCount = ?, @Json = ?, @SourceID = ?, @TopicID = ?, @UserID = ?", tweet.tweet.id, tweet.tweet.text, tweet.tweet.createdAt, tweet.tweet.retweets, tweet.tweet.replies, tweet.tweet.likes, tweet.tweet.quotes, tweet.tweet.json, sourceID, topicID, userID).fetchval()

    # Insert Key Phrases
    for keyPhrase in tweet.analysis.keyPhrases:
      cursor.execute("EXEC InsertKeyPhrases @Phrase = ?, @TweetID = ?", keyPhrase, tweetID).fetchval()
      
    # Insert Sentiment
    cursor.execute("EXEC InsertSentiment @Sentiment = ?, @TweetID = ?, @ConfidenceScoreID = ?", tweet.analysis.sentiment, tweetID, confidenceScoreID)
    connection.commit()

# Start program
run()
