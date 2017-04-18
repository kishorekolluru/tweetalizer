import json
import re


def clean_tweet(tweet_text):
    # tweet_text = 'RT @FillWerrell: #alsdkjfhlaks This is the best Volleyball Match ever \ud83d\ude02 https://t.co/hx1kcKjKws'
    tweet_text = re.sub(r'(\w+://\S+)', '', tweet_text)
    # remove those hashtags
    tweet_text = re.sub(r'(@[A-Za-z0-9_]+)', '', tweet_text)
    tweet_text = re.sub(r'(#[A-Za-z0-9]+)|([^0-9A-Za-z\s])', '', tweet_text)
    # remove key words that don't help any
    tweet_text = tweet_text.replace('\n', '').replace('\r', '')
    tweet_text = tweet_text.replace('RT', '')
    tweet_text = tweet_text.replace(':(', '')
    tweet_text = tweet_text.replace('=(', '')
    tweet_text = tweet_text.replace(':o(', '')
    tweet_text = tweet_text.replace(':-(', '')
    tweet_text = re.sub(r'\s{2,}', ' ', tweet_text)  # remove any extra spaces
    # tweet_text = tweet_text.replace('u\'https', '')

    tweet_text = tweet_text.lower().strip()
    return tweet_text


def extractTweetText(list_of_files, fileout):
    for filename in list_of_files:
        file = open(filename, mode='r')
        json_data = json.load(file)
        try:
            for tweet in json_data['twitterdata']:
                if 'end' not in tweet:
                    tweet_text = tweet['text']
                    tweet_text = clean_tweet(tweet_text)
                    if tweet_text != '':
                        print(tweet_text, end='\n')
                        print(tweet_text, file=fileout)
        except Exception as e:
            print("Exception occured :   {}".format(e.with_traceback()))
        file.close()
    fileout.close()


def main():
    list_of_files = [
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_positive.json',
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_wordpos_thread1.json',
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_wordpos_thread2.json',
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_wordpos_thread3.json',
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_wordpos_thread4.json',
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_wordpos_thread5.json',
        '/Users/kishorekolluru/PycharmProjects/cloud_project/raw_tweets_wordpos_thread6.json',
    ]

    clean_tweet_text_filename = 'cleanedtweets.txt'
    tweet_stopless_file = 'stopwordless_tweets.txt'
    fileout = open(clean_tweet_text_filename, mode='w', buffering=2000)

    # extractTweetText(list_of_files, fileout)

    # remove stop words
    clean_tweet_file = open(clean_tweet_text_filename, mode='r')
    stopless_file = open(tweet_stopless_file, mode='w', buffering=2000)

    from nltk.corpus import stopwords
    stop = set(stopwords.words('english'))

    for line in clean_tweet_file:
        tweet_line_clean = ''
        for word in line.lower().split():
            if word not in stop:
                tweet_line_clean += word + ' '
        print(tweet_line_clean, file=stopless_file, end='\n') if tweet_line_clean != '' else ''


if __name__ == '__main__': main()
