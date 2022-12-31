
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import requests
import time
import datetime

def twitter_get_users_by_usernames(user_string, bearer_token, user_fields):
    auth_header = {'Authorization' : f'Bearer {bearer_token}'}
    api_url_usersby = f'https://api.twitter.com/2/users/by?usernames={user_string}&user.fields={user_fields}'
    request = requests.get(api_url_usersby, headers = auth_header)
    request_left = int(request.headers['x-rate-limit-remaining'])
    return (request, request_left)

@retry(retry = retry_if_exception_type(KeyError), stop=stop_after_attempt(3), wait=wait_fixed(5))
def twitter_get_followers_by_users(id_, nxt_token, bearer_token):
    auth_header = {'Authorization' : f'Bearer {bearer_token}'}
    api_url_idfollowers = f'https://api.twitter.com/2/users/{id_}/followers?max_results=1000{nxt_token}'
    request = requests.get(api_url_idfollowers, headers = auth_header)
    request_left = int(request.headers['x-rate-limit-remaining'])
    return (request, request_left)

@retry(retry = retry_if_exception_type(KeyError), stop=stop_after_attempt(3), wait=wait_fixed(5))
def twitter_get_following_by_users(id_, nxt_token, bearer_token):
    auth_header = {'Authorization' : f'Bearer {bearer_token}'}
    api_url_idfollowers = f'https://api.twitter.com/2/users/{id_}/following?max_results=1000{nxt_token}'
    request = requests.get(api_url_idfollowers, headers = auth_header)
    request_left = int(request.headers['x-rate-limit-remaining'])
    return (request, request_left)

@retry(retry = retry_if_exception_type(KeyError), stop=stop_after_attempt(3), wait=wait_fixed(5))
def twitter_get_users_by_ids(followers_string, bearer_token, user_fields):
    auth_header = {'Authorization' : f'Bearer {bearer_token}'}
    api_url_ids = f'https://api.twitter.com/2/users?ids={followers_string}&user.fields={user_fields}'
    request = requests.get(api_url_ids, headers = auth_header)
    request_left = int(request.headers['x-rate-limit-remaining'])
    return (request,request_left)

# Function to swap bearer_token when request_left = 0
def catridge_reset(rate_limit_reset_time, bearer_tokens_catridge, bearer_tokens, request_left):
    if len(bearer_tokens_catridge) > 0: # There is still bearer_tokens avaialble in the bearer_tokens_catridge
        bearer_token = bearer_tokens_catridge.pop(0)
    else: # No more bearer_token available in the catridge.
        bearer_tokens_catridge = bearer_tokens.copy() # Reset bearer_tokens_catridge
        bearer_token = bearer_tokens_catridge.pop(0) # Pop bearer_token from new bearer_tokens_catridge
        if time.mktime(datetime.datetime.now().timetuple()) > rate_limit_reset_time: # 
            rate_limit_reset_time = time.mktime(datetime.datetime.now().timetuple()) + 1000
        else:
            rest_duration = rate_limit_reset_time - time.mktime(datetime.datetime.now().timetuple()) + (3*len(bearer_tokens_catridge))
            print(f'Bearer Token Catridge reset. Target to restart at {rate_limit_reset_time}. Estimated time remaining to restart = {rest_duration:.0f} seconds.')
            time.sleep(rest_duration)
            rate_limit_reset_time = time.mktime(datetime.datetime.now().timetuple()) + 1000
    return bearer_tokens_catridge, bearer_token, request_left, rate_limit_reset_time