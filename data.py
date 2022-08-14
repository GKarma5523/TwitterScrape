import json as js

queries = {
  'Delta': 'Delta Airlines -is:retweet -has:links lang:en',
  'Southwest': 'Southwest Airlines -is:retweet -has:links lang:en',
  'American': 'American Airlines -is:retweet -has:links lang:en'
}

class TweetAnalysis:
  def __init__(self, tweet, user, analysis):
    self.tweet = tweet
    self.user = user
    self.analysis = analysis

  def __repr__(self):
    return f"<TweetAnalysis \n Tweet: {self.tweet} \n User: {self.user} \n Analysis: {self.analysis}>\n"


class Analysis:
  def __init__(self, id, keyPhrases, sentiment, positive, neutral, negative):
    self.id = id
    self.keyPhrases = keyPhrases
    self.sentiment = sentiment
    self.positive = positive
    self.neutral = neutral
    self.negative = negative

  def __repr__(self):
    return "Id: {}, KeyPhrases: {}, Sentiment: {}, Positive: {}, Neutral: {}, Negative: {} \n".format(self.id, self.keyPhrases, self.sentiment, self.positive, self.neutral, self.negative)


class Tweet:
  def __init__(self, id, text, authorId, retweets, replies, likes, quotes, createdAt, source, json, topic):
    self.id = id
    self.text = text
    self.authorId = authorId
    self.retweets = retweets
    self.replies = replies
    self.likes = likes
    self.quotes = quotes
    self.createdAt = createdAt
    self.source = source
    self.json = js.dumps(json)
    self.topic = topic
  
  def __repr__(self):
    return "Id: {}, Text: {}, AuthorId: {}, Retweets: {}, Replies: {}, Likes: {}, Quotes: {}, Created_at: {}, Source: {}, Json: {}, Topic: {} \n".format(self.id, self.text, self.authorId, self.retweets, self.replies, self.likes, self.quotes, self.created_at, self.source, self.json, self.topic)


class User:
  def __init__(self, id, username, name, location):
    self.id = id
    self.username = username
    self.name = name
    self.location = location

  def __repr__(self):
    return "Id: {}, Username: {}, Name: {}, Location: {} \n".format(self.id, self.username, self.name, self.location)