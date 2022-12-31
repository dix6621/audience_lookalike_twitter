#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
main lambda

"""
from s3_functions import s3_pd_read_csv, s3_write_json
from twitter_functions import twitter_get_users_by, get_recent_tweets
from transform_functions import chunks,string_chunks, chunk_by_char_len, add_date

import time
import datetime

end_date = datetime.date.today()
# prev_date = end_date - datetime.timedelta(days=1)

def lambda_handler(event, context):
    
    ## extract user accounts data and their pinned tweets.
    
    dev_accs = s3_pd_read_csv('dataspartan-test-bucket','twitter-analytics/admin/developer-accounts/developer_accounts_details.csv')
    bearer_tokens = list(dev_accs['bearer_token'])
    
    projects_df = s3_pd_read_csv('dataspartan-test-bucket','twitter-analytics/admin/user-accounts/twitter_accounts_sui_ecosystem.csv')
    
    #change all twitter_username to lowercase
    projects_df['twitter_username'] = projects_df['twitter_username'].apply(lambda x: x.lower() if type(x) == str else x)
    
    #extract twitter_username from projects_df and remove NaN.
    users_accounts_username = projects_df['twitter_username'].dropna()
    
    #extract all unique twitter_usernames from social_df and put them into a list
    users_accounts_username_unique = list(dict.fromkeys(list(users_accounts_username)))
    
    # group users_accounts_username_unique into batch of 100s and convert them into chunks of strings
    users_groups = list(chunks(users_accounts_username_unique, 100))
    users_strings, counts = string_chunks(users_groups)
    
    auth_header = {'Authorization' : f'Bearer {bearer_tokens[0]}'}
    
    twitter_users_account_data, twitter_users_pinned_tweets = twitter_get_users_by(users_strings, auth_header)
    
    add_date(twitter_users_account_data,end_date)
    add_date(twitter_users_pinned_tweets,end_date)
    
    twitter_users_account_data_dict = {'user_data': twitter_users_account_data,}
    twitter_users_pinned_tweets_dict = {'pinned_tweets' : twitter_users_pinned_tweets,}
    
    s3_write_json('dataspartan-test-bucket', f'twitter-analytics/raw-data/user-accounts/results/twitter_users_account_data_{end_date}.json', twitter_users_account_data_dict)
    s3_write_json('dataspartan-test-bucket', f'twitter-analytics/raw-data/pinned-tweets/results/twitter_users_pinned_tweets_{end_date}.json', twitter_users_pinned_tweets_dict)

