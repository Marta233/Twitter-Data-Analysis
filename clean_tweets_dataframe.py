import pandas as pd
import json 
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
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']
        
        return df
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
         """
         drop duplicate rows
         """
         self.df = self.df.drop_duplicates().drop_duplicates(subset='original_text')
         return df
        
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
         """
         convert column to datetime
         """
         self.df['created_at'] = pd.to_datetime(self.df['created_at'], errors='coerce')
         self.df = self.df[self.df['created_at'] >= '2020-12-31' ]
         return self.df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
       """
       convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
       self.df['retweet_count'] = pd.to_numeric(self.df['retweet_count'], errors='coerce')
       self.df['favorite_count'] = pd.to_numeric(self.df['favorite_count'], errors='coerce')
       return self.df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        
        self.df = self.df.query("lang == 'en' ")
        
        return self.df
if __name__ == "__main__":
    tweets=[]
    my_dic= []
    for line in open('C:/Users/Marta/Twitter-Data-Analysis/data/Economic_Twitter_Data/Economic_Twitter_Data.json', 'r'):
        tweets.append(json.loads(line))
        my_dic.append(tweets[0])
    
    tweet_df = pd.DataFrame(my_dic)
    cleaner = Clean_Tweets(tweet_df)
    print(cleaner. drop_unwanted_column())