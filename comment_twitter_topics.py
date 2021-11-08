import twint
from datetime import datetime
import re
import uuid
from pymongo import MongoClient
from textblob import TextBlob
import sys
import requests
import json


from Config.settings import MONGO_URL, SLACK_WEBHOOK

client = MongoClient(MONGO_URL)
db = client.twitter_topics
collection = db['Startups']

commment_collection = db.startup_comments


from datetime import datetime, timedelta


last_week = datetime.now() - timedelta(9)
last_week_date = datetime.strftime(last_week, '%Y-%m-%d')
last_week_date



cut_off_h_diff = 6
search_h_diff = 24



def get_topics_from_db():

    try:
        cur = collection.find() 
        
        topic_data = list(collection.find({}, {"_id": 0, "topic_name":1, "tweet_url":1, "tweet_dict":1, "creator_id":1}))  

    except Exception as e:
        print(e)    
    return topic_data


def generate_cutoff_and_search_date(cut_off_h_diff, search_h_diff):
    curr_time = datetime.now()
    date = str(curr_time.date())
    hour = curr_time.hour
    cutoff_hour = max((hour - cut_off_h_diff),0)
    search_hour = max((hour - search_h_diff), 0)
    search_date_str = '%s %s:00:00' % (date, search_hour)
    cutoff_date_str = '%s %s:00:00' % (date, cutoff_hour)
    
    return search_date_str, cutoff_date_str


def get_twitter_handle_mentions(twitter_handle, search_date_str):
    try:
        num_tweets = 10000
        min_replies = 0
        verified = False
        search_term = '@%s' % twitter_handle
        mentions_df = get_tweets_from_search_term(search_term, min_replies, verified, num_tweets, search_date_str)
    except Exception as e:
        print(e)
    return mentions_df



def get_tweets_from_search_term(search_term, min_replies, verified, num_tweets, since_date_str):
    c = twint.Config()
    if verified:
        c.Verified = True
    else:
        c.Verified = False
    c.Min_replies = min_replies
    c.Since = since_date_str
    c.Pandas = True
    c.Search = search_term 
    c.Hide_output = True
    c.Limit = num_tweets
    c.Store_csv = True
    twint.run.Search(c)
    replies_df = twint.storage.panda.Tweets_df
    return replies_df


def cleanup_tweet(tweet, twitter_handle, num_reply=0):
    """
    This function takes in a tweet and then cleans it up by removing non alphanumericals etc
    """
    tweet_tokens = tweet.split()[num_reply:] # we ignore the first token which will always be the handle
    text_list = []
    for token in tweet_tokens:
        temp = ''.join([i for i in token if (i.isalpha() or (i in ['.',',', '..', '…', ':', ';', '?', '"', '-']) or i.isdigit())])        
        if '#' not in temp:
            if twitter_handle not in temp:
                text_list.append(temp.strip())

    tweet_text = ' '.join(text_list)
    tweet_text = re.sub(r"http\S+", "", tweet_text)

    return tweet_text


def get_twitter_handle_bio_details(twitter_handle):
    try:
        c = twint.Config()
        c.Username = twitter_handle
        c.Store_object = True
        c.User_full = False
        c.Pandas =True
        c.Hide_output = True
        twint.run.Lookup(c)
        user_df = twint.storage.panda.User_df.drop_duplicates(subset=['id'])
        try:
            user_id = list(user_df['id'])[0]
        except:
            user_id = 'NA'
        try:
            user_name = list(user_df['name'])[0]
        except:
            user_name = 'NA'
        try:
            user_bio = list(user_df['bio'])[0]
        except:
            user_bio = 'NA'
        try:
            user_profile_image_url = list(user_df['avatar'])[0]
        except:
            user_profile_image_url = 'NA'
        try:
            user_url = list(user_df['url'])[0]
        except:
            user_url = 'NA'
        try:
            user_join_date = list(user_df['join_date'])[0]
        except:
            user_join_date = 'NA'
        try:
            user_location = list(user_df['location'])[0]
        except:
            user_location = 'NA'
        try:
            user_following = list(user_df['following'])[0]
        except:
            user_following = 'NA'
        try:
            user_followers = list(user_df['followers'])[0]
        except:
            user_followers = 'NA'
        try:
            user_verified = list(user_df['verified'])[0]
        except:
            user_verified = 'NA'
    except Exception as e:
        print(e)
        user_name = 'NA'
        user_bio = 'NA'
        user_profile_image_url = 'NA'
        user_url = 'NA'
        user_join_date = 'NA'
        user_location = 'NA'
        user_following = 'NA'
        user_followers = 'NA'
        user_verified = 'NA'
    return user_name, user_bio, user_profile_image_url, user_url, user_location, user_following, user_followers, user_verified


def comment_dict(comments_df, twitter_handle):
    
    comments_dict = {}
    comments_list = comments_df['tweet']
    creator_names_list = comments_df['name']
    creator_handles_list = comments_df['username']
    comment_tweet_ids_list = comments_df['id']
    # bio_image_urls_list = comments_df['bio_image_urls']

    for i in range(len(comments_list)):
        ind = i+1
        name = 'comment_%s' % ind
        comment_text = comments_list[i]
        comment_text = cleanup_tweet(comment_text, twitter_handle, num_reply=1)

        comment_tweet_id = comment_tweet_ids_list[i]
        creator_name = creator_names_list[i]
        creator_handle = creator_handles_list[i]
        a,b,creator_bio_image_url,d,e,f,g,h = get_twitter_handle_bio_details(creator_handle)



        comment_dict = {
            'comment_text': comment_text,
            'comment_tweet_id': comment_tweet_id,
            'creator_name': creator_name,
            'creator_handle': creator_handle,
            'creator_bio_image_url': creator_bio_image_url,
        }       
        
        # Update the dict
        d1 = {name:comment_dict.copy()}
        comments_dict.update(d1)
        
    return comments_dict


def process_comment_content_dict(twitter_handle, tweet_id, tweet_url, comments_dict, tweet, topic_name):

    content_font_name = 'OpenSans-ExtraBold.ttf'
    content_primary_colour = '#550afb'
    content_secondary_colour = '#ffffff'

    content_details_dict = {
            'content_type': 'Topic Tweet Update',
            'content_bucket_name': " blovid-topics",
            'brand_bucket_name': "topic_comments_details",
            'content_id': str(uuid.uuid4()),
            'brand_id' : twitter_handle, # actually for the brand_id, we could just try to use the twitter handle as this would likely be unique... something to ponder
            'tweet_id' : tweet_id,
            "tweet" : tweet,
            "topic_name": topic_name,
            'content_font_name': content_font_name,
            'tweet_url' : tweet_url,
    #         'headline_dict': headline_dict, # For each brand we create a randomly generated ID for the brand
            'comments_dict': comments_dict, # For each brand we create a randomly generated ID for the brand
            'content_primary_colour': content_primary_colour,
            'content_secondary_colour': content_secondary_colour
       }
    
    return content_details_dict



def process_topic_comment():
    topic_data = get_topics_from_db()
    search_date_str, cutoff_date_str = generate_cutoff_and_search_date(cut_off_h_diff, search_h_diff)
    for topic_item in topic_data:

        tweet_url = topic_item['tweet_url']
        twitter_handle = tweet_url.split("/")[3]
        tweet_id = tweet_url.split("/")[5]

        search_dict = {"tweet_url": tweet_url}
        query = get_record_details(search_dict, commment_collection)

        if query == None:

            mentions_df = get_twitter_handle_mentions(twitter_handle, last_week_date)
            comments_df = mentions_df[mentions_df['conversation_id'] == tweet_id]
            # comments_df['is_english'] = comments_df['tweet'].apply(lambda row: TextBlob(row).detect_language())

            comments_df = comments_df[comments_df['language'] == "en"]

            comments_df = comments_df.sort_values(['nlikes'],ascending=False)[0:5]
            comments_df = comments_df.reset_index(drop=True)
            comments_dict = comment_dict(comments_df, twitter_handle)

            
            tweet = topic_item['tweet_dict']
            topic_name = topic_item['topic_name']
            
            content_details_dict = process_comment_content_dict(twitter_handle, tweet_id, tweet_url, comments_dict, tweet, topic_name)
            save_to_mongo_db(content_details_dict, commment_collection)
            notify_slack(content_details_dict, topic_name)

            print(content_details_dict)
            
        
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


def notify_slack(data, topic):
    """
        This sends the content_dict/ data to a slack channel
    """

    url =  SLACK_WEBHOOK
    
    message = (f'{data}')
    title = (f"New Incoming Message : {topic} :zap:")
    
    slack_data = {
        "username": f"COMMENTS - {topic}", #f'{topic}',
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


        
process_topic_comment()       