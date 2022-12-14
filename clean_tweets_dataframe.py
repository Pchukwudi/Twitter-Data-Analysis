import pandas as pd

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace = True)
        df = df[df['polarity'] != 'polarity']
        
        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows in the dataframe

        """
        df=df.drop_duplicates()  
        return df

    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert the created_at column to datetime
        """
        df['created_at'] = pd.to_datetime(df['created_at'])
        # df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        df['polarity'] = pd.to_numeric(df['polarity'], errors = 'raise', downcast = None)
        df['subjectivity']= pd.to_numeric(df['subjectivity'], errors = 'raise', downcast = None)
        df['favorite_count']= pd.to_numeric(df['fav_count'], errors = 'raise', downcast = None)
        df['retweet_count']= pd.to_numeric(df['retweet_count'], errors = 'raise', downcast = None)
        df['followers_count']= pd.to_numeric(df['followers_count'], errors = 'raise', downcast = None)
        df['friends_count']= pd.to_numeric(df['friends_count'], errors = 'raise', downcast = None)

        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from language column
        """
        df = df[df['lang']== 'en'] 
        
        return df