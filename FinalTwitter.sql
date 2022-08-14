DROP TABLE IF EXISTS Twitter.KeyPhrasesTweet
DROP TABLE IF EXISTS Twitter.UserTweet
DROP TABLE IF EXISTS Twitter.SentimentTweets
DROP TABLE IF EXISTS Twitter.Sentiments
DROP TABLE IF EXISTS Twitter.KeyPhrases
DROP TABLE IF EXISTS Twitter.TweetTopic
DROP TABLE IF EXISTS Twitter.ConfidenceScores
DROP TABLE IF EXISTS Twitter.Locations
DROP TABLE IF EXISTS Twitter.Tweet
DROP TABLE IF EXISTS Twitter.Source
DROP TABLE IF EXISTS Twitter.Topics
DROP TABLE IF EXISTS Twitter.[User]
DROP SCHEMA IF EXISTS Twitter

GO

CREATE SCHEMA Twitter

GO

CREATE TABLE Twitter.Tweet
(
    TweetID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    TwitterTweetID VARCHAR(255) NOT NULL,
    TweetText TEXT,
    CreateTime DATETIME2 NOT NULL,
    RetweetCount INT,
    ReplyCount INT,
    LikeCount INT,
    QuoteCount INT,
    [Json] TEXT,
    SourceID INT NOT NULL,
    TopicID INT NOT NULL,
    UserID INT NOT NULL
)

CREATE TABLE Twitter.[User]
(
    UserID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    TwitterUserID VARCHAR(255) NOT NULL,
    UserName VARCHAR(255) NOT NULL,
    [Name] VARCHAR(255) NOT NULL,
    [Location] VARCHAR(50)
)

CREATE TABLE Twitter.Topics
(
    TopicID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    Topic VARCHAR(50) NOT NULL
)

CREATE TABLE Twitter.Sentiments
(
    SentimentID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    Sentiment VARCHAR(20) NOT NULL
)

CREATE TABLE Twitter.SentimentTweets
(
    SentimentTweetID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    SentimentID INT NOT NULL,
    TweetID INT NOT NULL,
    ConfidenceScoreID INT NOT NULL
)

CREATE TABLE Twitter.ConfidenceScores
(
    ConfidenceScoreID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    Positive FLOAT NOT NULL,
    Neutral FLOAT NOT NULL,
    Negative FLOAT NOT NULL
)

CREATE TABLE Twitter.KeyPhrases
(
    KeyPhraseID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    Phrase VARCHAR(50) NOT NULL
)

CREATE TABLE Twitter.KeyPhrasesTweet
(
    PhraseTweetID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    KeyPhraseID INT NOT NULL,
    TweetID INT NOT NULL
)

CREATE TABLE Twitter.Source
(
    SourceID INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    SourceType VARCHAR(255) NOT NULL
)

GO

ALTER TABLE Twitter.Tweet
ADD CONSTRAINT fk_SourceID FOREIGN KEY (SourceID)
REFERENCES Twitter.Source(SourceID)

ALTER TABLE Twitter.Tweet
ADD CONSTRAINT fk_TopicID FOREIGN KEY (TopicID)
REFERENCES Twitter.Topics(TopicID)

ALTER TABLE Twitter.Tweet
ADD CONSTRAINT fk_UserID FOREIGN KEY (UserID)
REFERENCES Twitter.[User](UserID)

ALTER TABLE Twitter.SentimentTweets
ADD CONSTRAINT fk_SentimentID FOREIGN KEY (SentimentID)
REFERENCES Twitter.Sentiments(SentimentID)

ALTER TABLE Twitter.SentimentTweets
ADD FOREIGN KEY (TweetID)
REFERENCES Twitter.Tweet(TweetID)

ALTER TABLE Twitter.SentimentTweets
ADD CONSTRAINT fk_ConfidenceScoreID FOREIGN KEY (ConfidenceScoreID)
REFERENCES Twitter.ConfidenceScores(ConfidenceScoreID)

ALTER TABLE Twitter.KeyPhrasesTweet
ADD CONSTRAINT fk_KeyPhraseID FOREIGN KEY (KeyPhraseID)
REFERENCES Twitter.KeyPhrases(KeyPhraseID)

ALTER TABLE Twitter.KeyPhrasesTweet
ADD FOREIGN KEY (TweetID)
REFERENCES Twitter.Tweet(TweetID)


GO

CREATE OR ALTER PROCEDURE InsertTopics
    @Topic VARCHAR(255)
AS
    SET NOCOUNT ON
    IF EXISTS
    (
        SELECT Topic
        FROM Twitter.Topics
        WHERE Topic = @Topic
    )
    BEGIN
        SELECT TOP 1 TopicID
        FROM Twitter.Topics
        WHERE Topic = @Topic
        RETURN
    END
    ELSE
    BEGIN
        INSERT INTO Twitter.Topics
            (Topic)
        VALUES
            (@Topic)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    END
    SET NOCOUNT OFF;

GO

CREATE OR ALTER PROCEDURE InsertTweets
    @TwitterTweetID VARCHAR(255),
    @TweetText TEXT,
    @CreateTime DATETIME2,
    @RetweetCount INT,
    @ReplyCount INT,
    @LikeCount INT,
    @QuoteCount INT,
    @Json TEXT,
    @SourceID INT,
    @TopicID INT,
    @UserID INT
AS
    SET NOCOUNT ON
    IF EXISTS
    (
        SELECT TwitterTweetID
        FROM Twitter.Tweet
        WHERE TwitterTweetID = @TwitterTweetID
    )
    BEGIN
        SELECT TOP 1 TweetID
        FROM Twitter.Tweet
        WHERE TwitterTweetID = @TwitterTweetID
        RETURN
    END
    ELSE
    BEGIN
        INSERT INTO Twitter.Tweet
            (TwitterTweetID, TweetText, CreateTime, RetweetCount, ReplyCount, LikeCount, QuoteCount, [Json], SourceID, TopicID, UserID)
        VALUES
            (@TwitterTweetID, @TweetText, @CreateTime, @RetweetCount, @ReplyCount, @LikeCount, @QuoteCount, @Json, @SourceID, @TopicID, @UserID)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    END
    SET NOCOUNT OFF;
GO

CREATE OR ALTER PROCEDURE InsertUser
    @TwitterUserID VARCHAR(255),
    @UserName VARCHAR(50),
    @Name VARCHAR(255),
    @Location VARCHAR(50)
AS
    SET NOCOUNT ON
    IF EXISTS
    (
        SELECT TwitterUserID
        FROM Twitter.[User]
        WHERE TwitterUserID = @TwitterUserID
    )
    BEGIN
        SELECT TOP 1 UserID
        FROM Twitter.[User]
        WHERE TwitterUserID = @TwitterUserID
        RETURN
    END
    ELSE
    BEGIN
    INSERT INTO Twitter.[User]
        ([TwitterUserID], UserName, [Name], [Location])
    VALUES
        (@TwitterUserID, @UserName, @Name, @Location)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    END
    SET NOCOUNT OFF;
GO

CREATE OR ALTER PROCEDURE InsertKeyPhrases
    @Phrase VARCHAR(50),
    @TweetID INT
AS
    SET NOCOUNT ON
    IF EXISTS
    (
        SELECT Phrase
        FROM Twitter.KeyPhrases
        WHERE Phrase = @Phrase
    )
    BEGIN
        SELECT KeyPhraseID
        FROM Twitter.KeyPhrases
        WHERE Phrase = @Phrase
        INSERT INTO Twitter.KeyPhrasesTweet
            (KeyPhraseID, TweetID)
        VALUES
            (SCOPE_IDENTITY(), @TweetID)
        RETURN
    END
    ELSE
    BEGIN
    INSERT INTO Twitter.KeyPhrases
        (Phrase)
    VALUES 
        (@Phrase)
    INSERT INTO Twitter.KeyPhrasesTweet
        (KeyPhraseID, TweetID)
    VALUES
        (SCOPE_IDENTITY(), @TweetID)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    END
    SET NOCOUNT OFF;
GO

CREATE OR ALTER PROCEDURE InsertSource
    @SourceType VARCHAR(50)
AS
    SET NOCOUNT ON
    IF EXISTS
    (
        SELECT SourceType
        FROM Twitter.Source
        WHERE SourceType = @SourceType
    )
    BEGIN
        SELECT SourceID
        FROM Twitter.Source
        WHERE SourceType = @SourceType
        RETURN
    END
    ELSE
    BEGIN
    INSERT INTO Twitter.Source
        (SourceType)
    VALUES
        (@SourceType)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    END
    SET NOCOUNT OFF;
GO

CREATE OR ALTER PROCEDURE InsertSentiment
    @Sentiment VARCHAR(20),
    @TweetID INT,
    @ConfidenceScoreID INT
AS 
    SET NOCOUNT ON
    IF EXISTS
    (
        SELECT Sentiment
        FROM Twitter.Sentiments
        WHERE Sentiment = @Sentiment
    )
    BEGIN
        SET @Sentiment = (SELECT SentimentID
        FROM Twitter.Sentiments
        WHERE Sentiment = @Sentiment)
        INSERT INTO Twitter.SentimentTweets
            (SentimentID, TweetID, ConfidenceScoreID)
        VALUES
            (@Sentiment, @TweetID, @ConfidenceScoreID)
        RETURN
    END
    ELSE
    BEGIN
    INSERT INTO Twitter.Sentiments
        (Sentiment)
    VALUES
        (@Sentiment)

    DECLARE @NewID INT = SCOPE_IDENTITY()

    INSERT INTO Twitter.SentimentTweets
        (SentimentID, TweetID, ConfidenceScoreID)
    VALUES
        (@NewID, @TweetID, @ConfidenceScoreID)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    END
    SET NOCOUNT OFF;
GO

CREATE OR ALTER PROCEDURE InsertConfidenceScore
    @Positive FLOAT,
    @Neutral FLOAT,
    @Negative FLOAT
AS
BEGIN
    SET NOCOUNT ON
    INSERT INTO Twitter.ConfidenceScores
        (Positive, Neutral, Negative)
    VALUES
        (@Positive, @Neutral, @Negative)
        SELECT SCOPE_IDENTITY() AS ID
        RETURN
    SET NOCOUNT OFF
END;
GO
