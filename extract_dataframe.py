import json
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file, 'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        status_count = []
        for tweet in self.tweets_list:
            status_count.append(tweet['statuses_count'])
        
        return status_count
        
    def find_full_text(self)->list:
        text = []
        for tweet in self.tweets_list:
            text.append(tweet['full_text'])

        return text
       
    
    def find_sentiments(self, text)->list:
        polarity = []
        subjectivity = []
        for tweet in text:
            response_tweet = TextBlob(tweet).sentiment
            polarity.append(response_tweet.polarity)
            subjectivity.append(response_tweet.subjectivity)

        return polarity, subjectivity 

    def find_created_time(self)->list:
        created_at = []
        for tweet in self.tweets_list:
            created_at.append(tweet['created_at'])

        return created_at

    def find_source(self)->list:
        source = []
        for tweet in self.tweets_list:
            source.append(tweet['source'])
        return source

    def find_screen_name(self)->list:
        screen_name = []
        for tweet in self.tweets_list:
            screen_name.append(tweet['user']['screen_name'])
        return screen_name

    def find_followers_count(self)->list:
        followers_count = []
        for tweet in self.tweets_list:
            followers_count.append(tweet['user']['followers_count'])
        return followers_count

    def find_friends_count(self)->list:
        friends_count = []
        for tweet in self.tweets_list:
            friends_count.append(tweet['user']['friends_count'])
        return friends_count

    def is_sensitive(self)->list:
        is_sensitive = []
        for tweet in self.tweets_list:
            try:
                is_sensitive.append(tweet['possibly_sensitive'])
            except KeyError:
                is_sensitive.append([None])

        return is_sensitive

        # try:
        #     is_sensitive = [x['possibly_sensitive'] for x in self.tweets_list]
        # except KeyError:
        #     is_sensitive = [None]

        # return is_sensitive


    def find_favourite_count(self)->list:
        favourite_count = []
        for tweet in self.tweets_list:
            favourite_count.append(tweet['favorite_count'])

        return favourite_count
    
    def find_retweet_count(self)->list:
        retweet_count = []
        for tweet in self.tweets_list:
            retweet_count.append(tweet['retweet_count'])

        return retweet_count

    def find_hashtags(self)->list:
        hashtags = []
        for tweet in self.tweets_list:
            hashtags.append(tweet['entities']['hashtags'])

        return hashtags

    def find_mentions(self)->list:
        mentions = []
        for tweet in self.tweets_list:
            mentions.append(tweet['entities']['user_mentions'])

        return mentions

    def find_location(self)->list:
        location = []
        for tweet in self.tweets_list:
            try:
                location.append(tweet['user']['location'])

            except TypeError:
                location.append(None)
        
        return location

    def find_lang(self)->list:
        language = []
        for tweet in self.tweets_list:
            language.append(tweet['lang'])

        return language

    

    def get_tweet_df(self, save = True)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        print(f"friends_count {len(friends_count)}")
        sensitivity = self.is_sensitive()
        print(f"sensitivity {len(sensitivity)}")

        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()
        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("africa_twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above   