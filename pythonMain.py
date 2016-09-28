import sys, json, pyhdb, twitter, traceback


def getJSON():
    jsonString = [str(line) for line in sys.stdin].pop()
    print(jsonString)
    return json.loads(jsonString)

def writeSessionInfo(session):
    with open('endSession.bat', 'w') as f:
        f.write("%neoPath%\\neo close-db-tunnel --session-id " + session)

def createConnection(connectionInfo):
    return pyhdb.connect(host=connectionInfo['host'],port=connectionInfo['port'],user=connectionInfo['dbUser'],password=connectionInfo['dbUserPassword'])

def test(connection):
    cursor = connection.cursor()
    result = cursor.execute("SELECT 'THIS IS A TEST' AS COLUMN_NAME FROM DUMMY")
    print(cursor.fetchone())

def runSQLIgnoringException(SQL, connection):
    cursor = connection.cursor()
    try:
        cursor.execute(SQL)
    except:
        pass

def demo(connection):
    def execute(connection):
        cursor = connection.cursor()
        cursor.execute('CREATE COLUMN TABLE IPHONE_NEWS("File_Name" NVARCHAR(20), "File_Content" BLOB, PRIMARY KEY("File_Name"))')
        with open('iPhone-News.pdf', 'rb') as pdf:
            content = pdf.read()
            cursor.execute("INSERT INTO IPHONE_NEWS VALUES(?,?)",('iPhone-News.pdf',content))
        cursor.execute('Create FullText Index "PDF_FTI" On "IPHONE_NEWS"("File_Content") TEXT ANALYSIS ON CONFIGURATION \'EXTRACTION_CORE_VOICEOFCUSTOMER\'')
        runSQLIgnoringException("COMMIT", connection)
        cursor.execute("SELECT \"TA_TYPE\", ROUND(\"SENTIMENT_VALUE\"/ \"TOTAL_SENTIMENT_VALAUE\" * 100,2) AS \"SENTIMENT_VALAUE_PERCENTAGE\" FROM ( SELECT \"TA_TYPE\", SUM(\"TA_COUNTER\") AS \"SENTIMENT_VALUE\" FROM \"$TA_PDF_FTI\" where TA_TYPE in('WeakPositiveSentiment','StrongPositiveSentiment','NeutralSentiment','WeakNegativeSentiment','StrongNegativeSentiment','MajorProblem','MinorProblem') GROUP BY \"TA_TYPE\" ) AS TABLE1,( SELECT SUM(\"TA_COUNTER\") AS \"TOTAL_SENTIMENT_VALAUE\" FROM \"$TA_PDF_FTI\" where TA_TYPE in ('WeakPositiveSentiment','StrongPositiveSentiment','NeutralSentiment','WeakNegativeSentiment','StrongNegativeSentiment','MajorProblem','MinorProblem') ) AS TABLE2")
        x = cursor.fetchall()
        print(x)
    def cleanUp(connection):
        runSQLIgnoringException("DROP TABLE IPHONE_NEWS", connection)
        runSQLIgnoringException("DROP INDEX \"$TA_PDF_FTI\"", connection)
    cleanUp(connection)
    try:
        execute(connection)
    except Exception as e:
        print(e)

def upload(tweet, connection):
    runSQLIgnoringException('CREATE COLUMN TABLE TWEETS ( TWEET NVARCHAR(150), USER NVARCHAR(50), LOCATION_NAME NVARCHAR(50), COUNTRY_CODE NVARCHAR(10), DATE_FROM NVARCHAR(10), DATE_TO NVARCHAR(10))', connection)
    cursor = connection.cursor()
    print("Inserting")
    try:
        print(repr(tweet))
    except Exception as e:
        tb = traceback.format_exc()
        print(tb)
    else:
        if tweet != None:
            cursor.execute("INSERT INTO TWEETS VALUES(?,?,?,?,?,?)", tweet)
            cursor.close()
            runSQLIgnoringException('COMMIT', connection)

def parse(property):
    try:
        property = str(property)
    except Exception as e:
        property = ''
    return property

def doTwitterStream(api):
    trackString = '@realDonaldTrump @HillaryClinton'
    y = api.GetStreamFilter(track=trackString)
    return y

def processTweet(content, startDate, endDate):
    tweet_text = parse(content.text)
    if content.user and tweet_text != '':
        screen_name = parse(content.user.screen_name)
        place_name = ''
        if content.user.location != None:
            place_name = parse(content.user.location)
            place_country_code = ''
            if content.user.geo_enabled:
                if content.place != None:
                    place = content.place
                    place_name = parse(place['full_name'])
                    place_country_code = parse(place['country_code'])
            return (tweet_text, screen_name, place_name, place_country_code, startDate, endDate)

def processTweet2(content):
    if content == None or not content.keys:
        return None
    if 'user' in content.keys() and 'text' in content.keys():
        user = content['user']
        tweet_text = parse(content['text'])
        if tweet_text != '':
            if 'screen_name' in user.keys():
                screen_name = parse(user['screen_name'])
            place_name = ''
            if 'location' in user.keys():
                place_name = parse(user['location'])
                place_country_code = ''
                if 'geo_enabled' in user.keys():
                    if user['geo_enabled']:
                        if 'place' in content.keys():
                            place = content['place']
                            if place != None and 'full_name' in place.keys():
                                place_name = parse(place['full_name'])
                                place_country_code = parse(place['country_code'])
            return (tweet_text, screen_name, place_name, place_country_code, None, None)
    return None

def doTwitterStatus(api):        
    x = u"@timkaine"
    phxGeoCode = '33.436344,-112.006203,50mi'
    startDate = '2016-05-01'
    endDate = '2016-09-15'
    y = api.GetSearch(count=100,term=x,since=startDate,until=endDate)
    for content in y:
        yield processTweet(content, startDate, endDate)


def getTrumpReTweets(api):
    y = api.GetUserTimeline(screen_name='realDonaldTrump',count=200)
    i = 0
    for content in y:
        if i < 5:
            try:
                if content.id != None:
                    x = api.GetRetweets(statusid=content.id)
                    for retweet in x:
                        if retweet.user != None:
                            print(retweet.user)
                        if retweet.place != None:
                            print(retweet.place)
                        if retweet.retweeted != None:
                            print(retweet.retweeted)
                        print(retweet.text)
            except Exception as e:
                print(e)
            i += 1

def processStream(api, connection):
    try:
        for tweet in doTwitterStream(api):
            if tweet != None:
                upload(processTweet2(tweet), connection)
    except Exception as e:
        tb = traceback.print_exc()
        print(tb)

def processTwitterStatus(api, connection):
    try:
        for tweet in doTwitterStatus(api):
            upload(tweet, connection)
    except Exception as e:
        print(e)

def main(connection):
#    test(connection)
#    demo(connection)
    api = twitter.Api(consumer_key='your_consumer_key',consumer_secret='your_consumer_secret',access_token_key='your_access_token_key',access_token_secret='your_access_token_secret')
#    getTrumpReTweets(api)
#    processStream(api, connection)
#    processTwitterStatus(api, connection)

if __name__ == "__main__":
    connectionJSON = getJSON()['result']
    connection = createConnection(connectionJSON)
    main(connection)
    writeSessionInfo(connectionJSON['sessionId'])
