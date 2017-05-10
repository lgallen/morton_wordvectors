#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May  4 06:47:16 2017

@author: lucas
"""

import tweepy
import pandas as pd
import numpy as np
import time


def authenticate_twitter_api(
        credentials_path="../private_data/twitter_credentials.csv"):
    """Athenticates with twitter api
    :credentials_path: path to a csv with a single row consisting
    of an entry for consumer_key, consumer_secret, access_key,
    and access_secret
    """
    credentials_df = pd.read_csv(str(credentials_path))
    consumer_key = credentials_df['consumer_key'][0]
    consumer_secret = credentials_df['consumer_secret'][0]
    access_key = credentials_df['access_key'][0]
    access_secret = credentials_df['access_secret'][0]
    OAUTH_KEYS = {'consumer_key': consumer_key,
                  'consumer_secret': consumer_secret,
                  'access_token_key': access_key,
                  'access_token_secret': access_secret}
    auth = tweepy.OAuthHandler(OAUTH_KEYS['consumer_key'],
                               OAUTH_KEYS['consumer_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)
    return (auth, api)


def get_twitter_followers(users_list,
                          credentials_path="./private_data/twitter_credentials.csv",
                          followers_path="./private_data/twitter_followers.csv"):
    """Retrieves Twitter followers given a list of Twitter usernames.
    :users_list: list of Twitter usernames
    :credentials_path: dataframe of twitter api credentials
    :followers_path: path to write csv of followers to
    """
    auth, api = authenticate_twitter_api(credentials_path)
    followers_df = pd.DataFrame()
    for name in users_list:
        # Build list of followers, storing in csv after each
        # user in case it breaks at some point.
        print(name)
        count = 0
        ids = []
        for page in tweepy.Cursor(api.followers_ids, screen_name=name).pages():
            ids.extend(page)
            print(count)
            count += 1
            time.sleep(70)
        current_followers_df = pd.DataFrame()
        current_followers_df['userId'] = ids
        followers_df = pd.concat([followers_df, current_followers_df])
        followers_df.to_csv(followers_path, index=False)
    return(followers_df)


def get_twitter_users_data(users_list,
                           credentials_path="./private_data/twitter_credentials.csv",
                           users_path="./private_data/twitter_users.csv"):
    """Retrieves Twitter followers given a list of Twitter usernames.
    :users_list: list of Twitter ids
    :credentials_path: dataframe of twitter api credentials
    :followers_path: path to write csv of followers to
    """
    auth, api = authenticate_twitter_api(credentials_path)
    number_users = len(users_list)
    user_data = []
    count = 0
    # Iterate through users in groups of 100
    for i in range(np.int(number_users/100)):
        current_users = users_list[100*i:min(100*(i+1), len(users_list))]
        try:
            user_data + api.lookup_users(current_users)
        except:
            pass
        count += 1
        print(count)
        users_df = pd.DataFrame()
        users_df['userData'] = user_data
        users_df.to_csv(users_path, index=False)
        time.sleep(70)
    return(users_df)


def retrieve_tweet(tweepy_user):
    """Returns twitter user's latest status if it exists
    :tweepy_user: tweepy api user object
    """
    try:
        return(tweepy_user.status.text)
    except:
        return("")


def parse_twitter_profiles(users):
    """Parses data returned from Tweepy and returns only
    necessary data for this project
    Returns pandas dataframe with parsed data
    :users: Pandas series of type tweepy.models.User
    """
    twit_id = users.apply(lambda x: x.id)
    screen_name = users.apply(lambda x: x.screen_name)
    name = users.apply(lambda x: x.name)
    location = users.apply(lambda x: x.location)
    language = users.apply(lambda x: x.lang)
    description = users.apply(lambda x: x.description)
    tweet = users.apply(lambda x: retrieve_tweet(x))
    parsed_profiles = pd.DataFrame()
    parsed_profiles['twit_id'] = twit_id
    parsed_profiles['screen_name'] = screen_name
    parsed_profiles['name'] = name
    parsed_profiles['location'] = location
    parsed_profiles['language'] = language
    parsed_profiles['description'] = description
    parsed_profiles['tweet'] = tweet
    return(parsed_profiles)
