"""
main lambda

"""
from s3_functions import s3_pd_read_csv, s3_write_json
from functions_twitter import twitter_get_users_by, get_recent_tweets
from functions_transform import chunks,string_chunks, chunk_by_char_len, add_date

import time
import datetime

end_date = datetime.date.today()
# end_date_sat = f'{end_date}T00:00:00.000Z'
# start_date = end_date - datetime.timedelta(days = 5)
# start_date_mon = f'{start_date}T00:00:00.000Z'
# prev_date = end_date - datetime.timedelta(days = 1)

def lambda_handler(event, context):
    
    dev_accs = s3_pd_read_csv('dataspartan-test-bucket','twitter-analytics/admin/developer-accounts/developer_accounts_details.csv')
    bearer_tokens = list(dev_accs['bearer_token'])
    
    projects_df = s3_pd_read_csv('dataspartan-test-bucket','twitter-analytics/admin/user-accounts/twitter_accounts_sui_ecosystem.csv')
    
    #change all twitter_username to lowercase
    projects_df['twitter_username'] = projects_df['twitter_username'].apply(lambda x: x.lower() if type(x) == str else x)
    
    #extract twitter_username from projects_df and remove NaN.
    users_accounts_username = projects_df['twitter_username'].dropna()
    
    #extract all unique twitter_usernames from social_df and put them into a list
    users_accounts_username_unique = list(dict.fromkeys(list(users_accounts_username)))
    
    unique_twitter_users_fr_ls = [f'from:{twitter_user}' for twitter_user in users_accounts_username_unique]
    
    char_chunks = chunk_by_char_len(unique_twitter_users_fr_ls, 512)
    
    bearer_tokens_catridge = bearer_tokens.copy()
    bearer_token = bearer_tokens_catridge.pop(0)
    
    rate_limit_reset_time = time.mktime(datetime.datetime.now().timetuple()) + 1000
    all_response_data = []
    tweet_count = 0
    request_left = 450
    for index, query in enumerate(char_chunks):
        nxt_page = True
        nxt_token = ''
        while nxt_page:
            if request_left > 0:
                try:
                    response_json, request_headers, status_code = get_recent_tweets(query, bearer_token, nxt_token)
                    request_left = int(request_headers['x-rate-limit-remaining'])
                    if status_code == 200:
                        all_response_data += response_json['data']
                    response_meta = response_json['meta']
                    if 'next_token' in response_meta.keys():
                        nxt_page = True
                        nxt_token = f'&pagination_token={response_meta["next_token"]}'
                    else:
                        nxt_page = False
                    tweet_count = len(all_response_data)
                except KeyError:
                    if status_code == 503:
                        print('Twitter is temporarily over capacity. Take a break!')
                        time.sleep(30)
                        nxt_page = True
                    else:
                        error_detail = f'Error found in main loop.'
                        print(error_detail)
                        nxt_page = False
                print(f'chunk: {index}, request_left: {request_left}, tweet_count: {tweet_count}')
            else:
                request_reset = float(request_headers['x-rate-limit-reset'])
                rate_limit_reset_time = min(request_reset,rate_limit_reset_time)
                print(rate_limit_reset_time)
                if len(bearer_tokens_catridge) > 0:
                    bearer_token = bearer_tokens_catridge.pop(0)
                    print(f'token_catridge left {len(token_catridge)} bullets.')
                    request_left = 450
                else:
                    bearer_tokens_catridge = bearer_tokens.copy()
                    bearer_token = bearer_tokens_catridge.pop(0)
                    print('bearer_tokens_catridge reset')
                    if time.mktime(datetime.datetime.now().timetuple()) > rate_limit_reset_time:
                        print(f'reset completed {time.mktime(datetime.datetime.now().timetuple()) - rate_limit_reset_time}s ago.')
                        rate_limit_reset_time = time.mktime(datetime.datetime.now().timetuple()) + 1000
                        request_left = 450
                    else:
                        rest_duration = rate_limit_reset_time - time.mktime(datetime.datetime.now().timetuple()) + (5*len(bearer_tokens_catridge))
                        print(rate_limit_reset_time)
                        print(rest_duration)
                        time.sleep(rest_duration)
                        print(f'token_catridge left {len(bearer_tokens_catridge)} bullets.')
                        rate_limit_reset_time = time.mktime(datetime.datetime.now().timetuple()) + 1000
                        request_left = 450
                        
    add_date(all_response_data,end_date)
    
    all_response_data_json = {'recent_tweets' : all_response_data}
    
    s3_write_json('dataspartan-test-bucket', f'twitter-analytics/raw-data/recent-tweets/results/twitter_recent_tweets_{end_date}.json', all_response_data_json)
