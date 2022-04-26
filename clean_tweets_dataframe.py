from typing_extensions import Self
import pandas as pd
import json 
class CleanTweets:
    """
    This class is responsible for cleaning the twitter dataframe
    Returns:
    --------
    A dataframe
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df: pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        columns =['id', 'id_str', 'text', 'truncated', 'entities', 'source', 'in_reply_to_status_id',
        'in_reply_to_status_id_str', 'in_reply_to_screen_name', 'user', 'geo', 'coordinates', 'place', 'contributors', 'retweeted_status', 'is_quote_status', 'retweet_count', 'favorite_count', 'favorited', 'retweeted', 'lanf']
        unwanted_rows = []
        for columnName in columns:
            unwanted_rows += df[df[columnName] == columnName]

        df.drop(unwanted_rows, inplace=True)
        df.reset_index(drop=True, inplace=True)

        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
       """
       drop duplicate rows
       """
       df.drop_duplicates(inplace=True)
       df.reset_index(drop=True, inplace=True)
       return df
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        self.df['created_at'] = pd.to_datetime(self.df['created_at'], errors='coerce')
        
        self.df = self.df[self.df['created_at'] >= '2020-12-31' ]
        
        return self.df
    
    def convert_to_numbers(self)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
       # self.df['polarity'] = pd.to_numeric(self.df['polarity'], errors='coerce')
        self.df['retweet_count'] = pd.to_numeric(self.df['retweet_count'], errors='coerce')
        self.df['favorite_count'] = pd.to_numeric(self.df['favorite_count'], errors='coerce')
        self.df['in_reply_to_status_id'] = pd.to_numeric(self.df['in_reply_to_status_id'], errors='coerce')
        
        return self.df
    
    def remove_non_english_tweets(self, df: pd.DataFrame) -> pd.DataFrame:
         """
         remove non english tweets from lang
         """
         index_names = df[df['lang'] != "en"].loc[:26]
         df.drop(index_names, inplace=True)
         df.reset_index(drop=True, inplace=True)
         return df

if __name__ == "__main__":
    tweets=[]
    my_dic= []
    for line in open('./data/Economic_Twitter_Data.json', 'r'):
        tweets.append(json.loads(line))
        my_dic.append(tweets[0])
    
    tweet_df = pd.DataFrame(my_dic)
    
   
