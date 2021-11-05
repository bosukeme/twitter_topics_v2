from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from time import sleep
from selenium.webdriver.chrome.options import Options

options=Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--headless')
options.add_experimental_option('excludeSwitches', ['enable-logging'])


from Config.settings import MONGO_URL, SLACK_WEBHOOK

import sys
import requests
import json
from ordered_set import OrderedSet
import pandas as pd
from pymongo import MongoClient
import uuid
import re, os

from datetime import datetime, timedelta
import twint
from textblob import TextBlob

search_day = datetime.now() - timedelta(21)
search_day_str = datetime.strftime(search_day, '%Y-%m-%d')

num_tweets = 700

client = MongoClient(MONGO_URL)
db = client.twitter_topics




def open_browser(topic_url):
    """
        This starts the chrome browser and opens the url
    """
    ## windows
    # PATH = 'C://Users/hp/Desktop/Chrome Driver/chromedriver.exe'

    ## linux
    PATH = "/usr/lib/chromium-browser/chromedriver"

    try:
        driver = webdriver.Chrome(PATH, options=options)

        driver.get(topic_url)
        sleep(5)
    except Exception as e:
        print(e)

    return driver


def get_topic_and_sub_topic(driver):
    """
        This inpects the title of the topic section
    """
    try:
        class_path_text = "[class='css-1dbjc4n r-1ydw1k6 r-usiww2']"
        topic_section = driver.find_elements_by_css_selector(class_path_text)
        sleep(4)
        topic_section = topic_section[0].text
        section_split = topic_section.split("\n")
        
        topic = section_split[0]
        sub_topic = section_split[1]
    except Exception as e:
        print(e)

    return topic



def scroll_down_twitter(driver):
    """
        This implements an infinite scroll on twitter and stops at the end of the page
    """
    # Get scroll height

    try:
        last_height = driver.execute_script("return document.body.scrollHeight")

        new_height = 10

        tweet_urls = []
        
        while True:

            driver.execute_script(f"window.scrollTo(0, {new_height});")
            sleep(5)
            
            tag_sections = driver.find_elements_by_tag_name("a")

            tags = [item.get_attribute('href') for item in tag_sections]
            ids = [item for item in tags if "status" in item and "photo" not in item]
            sleep(3)
            
            tweet_urls.append(ids)
            

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script(f"return {new_height+1000}")
            print(new_height)
            if new_height > last_height:
                break
    except Exception as e:
        print(e)
        
    return tweet_urls


def format_tweet_data(tweet_urls):
    """
        This splits a list of list into a single ordered list 
    """
    try:
        tweet_urls = [a for b in tweet_urls for a in b]
        tweet_urls = list(OrderedSet(tweet_urls))
    except Exception as e:
        print(e)
    return  tweet_urls



def twint_to_pandas(columns):
    return twint.output.panda.Tweets_df[columns]


def get_latest_tweets_from_handle(username, num_tweets, date):
    """
        This uses twint to get all the tweets made by the username/handle
    """
    c = twint.Config()
    c.Username = username
    c.Limit = num_tweets
    c.Pandas = True
    c.Since = date
    c.Hide_output = True

    twint.run.Search(c)
    
    try:
        tweet_df = twint_to_pandas(['id', 'conversation_id', 'date', 'tweet', 'language', 'hashtags', 
               'username', 'name', 'link', 'urls', 'photos', 'video',
               'thumbnail', 'retweet', 'nlikes', 'nreplies', 'nretweets', 'source'])
    except Exception as e:
        print(e)
        tweet_df = pd.DataFrame()
        
    return tweet_df


def get_tweet_id_and_handle_from_url(tweet_url):
    """
    This function takes a tweet_url and then returns the tweet ID and tweet handle
    """
    try:
        split_list = tweet_url.split('/')
        twitter_handle = split_list[3]
        tweet_id = split_list[5]
    except Exception as e:
        print(e)
    return tweet_id, twitter_handle


def cleanup_tweet(tweet, twitter_handle, num_reply=0):
    """
    This function takes in a tweet and then cleans it up by removing non alphanumericals etc
    """
    try:
        tweet_tokens = tweet.split()[num_reply:] # we ignore the first token which will always be the handle
        text_list = []
        for token in tweet_tokens:
            temp = ''.join([i for i in token if (i.isalpha() or (i in ['.',',', '..', '…', ':', ';', '?', '"', '-']) or i.isdigit())])        
            if '#' not in temp:
                if twitter_handle not in temp:
                    text_list.append(temp.strip())

        tweet_text = ' '.join(text_list)
        tweet_text = re.sub(r"http\S+", "", tweet_text)
    except Exception as e:
        print(e)
    return tweet_text



def process_tweet_urls(tweet_urls, topic):
    content_bucket_name = "blovid-topics"
    content_font_name = 'OpenSans-ExtraBold.ttf'
    content_primary_colour = '#550afb'
    content_secondary_colour = '#ffffff'
    
    for tweet_url in tweet_urls[:12]:
        
        collection = db[topic]
        search_dict = {'tweet_url': tweet_url}
        
        query  = get_record_details(search_dict, collection, find_one=True)
        if query  == None:
            tweet_id, twitter_handle = get_tweet_id_and_handle_from_url(tweet_url)

            date = search_day_str
            username = twitter_handle

            df = get_latest_tweets_from_handle(username, num_tweets, date)
            try:
                main_dict = df[df['id'] == tweet_id].to_dict("records")[0]
            except:
                main_dict = {}
            
            print(main_dict)
            tweet = main_dict['tweet']
            cleaned_tweet_text = cleanup_tweet(tweet, twitter_handle, num_reply=0)
            
            if len(tweet.split()) - len(cleaned_tweet_text.split()) < 7:

                language = TextBlob(tweet)
                language = language.detect_language()
                if language == "en":

                    tweet_dict = {
                        'tweet_text': cleaned_tweet_text,
                                }

                    content_details_dict = {
                        'content_type': 'Single Tweet',
                        'content_bucket_name': content_bucket_name,
                        'tweet_creator_bucket_name': main_dict['name'],
                        'content_id': str(uuid.uuid4()),
                        'creator_id' : twitter_handle, 
                        'content_font_name': content_font_name, #standard
                        'tweet_dict': tweet_dict, 
                        'content_primary_colour': content_primary_colour, # standard
                        'content_secondary_colour': content_secondary_colour, # standard
                        'tweet_url': tweet_url
                    }

                    print(content_details_dict)
                    save_to_mongo_db(content_details_dict, collection)






def get_record_details(search_dict, collection, find_one=True):
    """
        This searches through mongodb for a single record
    """
    try:
        query = collection.find_one(search_dict) if find_one else collection.find(search_dict)
        return query
    except Exception as e:
        print(e)
        return None


def insert_records(collection, record):
    """
        This inserts a single record to mongo db
    """
    try:
        collection.insert_one(record)
    except Exception as e:
        print(e)

def save_to_mongo_db(data, collection):
    """
        This saves the record to mongo db
    """
    insert_records(collection, data)
    cur = collection.count_documents({})
    print(f"we have {cur} entries")




def process_tweet_urls_v2(tweet_urls, topic):
    """
        This takes a tweet url and processes it to get the tweets 
    """
    try:
        collection = db[topic]
        
        tweet_df_list = []
        for tweet_url in tweet_urls[:15]:
            
            search_dict = {'tweet_url': tweet_url}

            query  = get_record_details(search_dict, collection, find_one=True)
            
            if query == None:

                tweet_id, twitter_handle = get_tweet_id_and_handle_from_url(tweet_url)
                date = search_day_str
                username = twitter_handle

                df = get_latest_tweets_from_handle(username, num_tweets, date)
                try:
                    main_dict = df[df['id'] == tweet_id].to_dict("records")[0]
                except:
                    main_dict = {}

                tweet_df_list.append(main_dict)
                
        try:    
            tweet_df = pd.DataFrame(tweet_df_list)
            tweet_df = tweet_df.sort_values(by ='photos')
        except:
            tweet_df = pd.DataFrame()

    except Exception as e:
        print(e)
    return tweet_df

def process_content_dict(tweet_df, topic):

    try:
        content_bucket_name = "blovid-topics"
        tweet_creator_bucket_name = "topic-users-details"
        content_font_name = 'OpenSans-ExtraBold.ttf'
        content_primary_colour = '#550afb'
        content_secondary_colour = '#ffffff'

            
        for item in tweet_df.to_dict("records"):
        
            tweet_url = item['link']
            collection = db[topic]
            tweet = item['tweet']
            twitter_handle = item['username']
            
            cleaned_tweet_text = cleanup_tweet(tweet, twitter_handle, num_reply=0)

            if len(tweet.split()) - len(cleaned_tweet_text.split()) < 7:

                language = TextBlob(tweet)
                language = language.detect_language()
                if language == "en":

                    tweet_dict = {
                        'tweet_text': cleaned_tweet_text,
                                }

                    content_details_dict = {
                        'content_type': 'Topic Tweet',
                        'content_bucket_name': content_bucket_name,
                        'tweet_creator_bucket_name': tweet_creator_bucket_name,
                        'content_id': str(uuid.uuid4()),
                        'creator_id' : twitter_handle, 
                        'content_font_name': content_font_name, #standard
                        'tweet_dict': tweet_dict, 
                        'content_primary_colour': content_primary_colour, # standard
                        'content_secondary_colour': content_secondary_colour, # standard
                        'tweet_url': tweet_url,
                        "topic_name": topic
                    }
                    
                    print(content_details_dict)
                    save_to_mongo_db(content_details_dict, collection)
                    notify_slack(content_details_dict, topic)

    except Exception as e:
        print(e)



def notify_slack(data, topic):
    """
        This sends the content_dict/ data to a slack channel
    """

    url =  SLACK_WEBHOOK
    
    message = (f'{data}')
    title = (f"New Incoming Message : {topic} :zap:")
    
    slack_data = {
        "username": f'{topic}',
        "attachments": [
            {
                "color":  "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
        
    return None





def process_topic():
    topic_url = "https://twitter.com/i/topics/849075881653846016"
    # topic_url = "https://twitter.com/i/topics/1242158204751990788?s=12"

    driver = open_browser(topic_url)
    topic = get_topic_and_sub_topic(driver)
    tweet_urls = scroll_down_twitter(driver)
    tweet_urls = format_tweet_data(tweet_urls)
    # process_tweet_urls(tweet_urls, topic)
    tweet_df = process_tweet_urls_v2(tweet_urls, topic)
    process_content_dict(tweet_df, topic)
    driver.quit()


process_topic()