#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
twitter related functions
"""
import requests

# https://developer.twitter.com/en/docs/twitter-api/users/lookup/api-reference/get-users-by
# Retrieve multiple users with usernames
def twitter_get_users_by(users_strings, auth_header):
    users_accounts_data = []
    users_pinned_tweets = []
    for users_string in users_strings:
        api_url = f'https://api.twitter.com/2/users/by?usernames={users_string}&expansions=pinned_tweet_id&tweet.fields=author_id,created_at,public_metrics&user.fields=created_at,public_metrics,verified,description,location,pinned_tweet_id'
        twitter_acc_data = requests.get(api_url, headers = auth_header).json()
        users_accounts_data+=twitter_acc_data['data']
        users_pinned_tweets+=twitter_acc_data['includes']['tweets']
    return users_accounts_data, users_pinned_tweets
    
def get_recent_tweets(query, bearer_token, nxt_token):
    headers = {
    'Authorization': f"Bearer {bearer_token}",
    }
    response = requests.get(f'https://api.twitter.com/2/tweets/search/recent?query={query}&max_results=100&tweet.fields=attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,edit_history_tweet_ids,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld&expansions=attachments.media_keys,attachments.poll_ids,author_id,edit_history_tweet_ids,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id&media.fields=alt_text,duration_ms,height,media_key,preview_image_url,public_metrics,type,url,variants,width&poll.fields=duration_minutes,end_datetime,id,options,voting_status&user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type{nxt_token}', headers=headers)
    response_json = response.json()
    request_headers = response.headers
    status_code = response.status_code
    return response_json, request_headers, status_code

