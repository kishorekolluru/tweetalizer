# Group 8
# Nanda Kishore Kolluru - 800970494
# Jay Shah - 800326050
# This file is used to compute the accuracy of the classfied and labeled tweets.
import re
import sys

from pyspark import SparkContext

def clean_tweet(tweet_text):
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
    tweet_text = tweet_text.lower().strip()

    # tweet_text = slash_stop_words(tweet_text)  # remove stop words
    return tweet_text



def reduce_func(tuple):
    res_list = list(tuple[1])# value
    if len(res_list) > 1:
        total_accm.add(1)
        a = int(res_list[0])
        b = int(res_list[1])
        if a == b:
            correct_accm.add(1)
            return (tuple[0], res_list)
    return (tuple[0], res_list)

correct_accm = 0
total_accm = 0

if __name__ == "__main__":
    sc = SparkContext(appName="Efficiency")

    total_accm = sc.accumulator(0)
    correct_accm = sc.accumulator(0)

    csvTweets_rdd = sc.textFile(sys.argv[1])
    tupletweets_rdd = sc.textFile(sys.argv[2])
    tuple_csvtweets_rdd = csvTweets_rdd.map(lambda val: (clean_tweet(val.rsplit(",", 1)[0]), int(val.rsplit(",", 1)[1].strip())))

    # print "THE TUPLE FILE IS  " + str(tupletweets_rdd.take(3))
    tup_rdd = tupletweets_rdd.map(lambda val: (clean_tweet(val.rsplit(",", 1)[0]), int(val.rsplit(",", 1)[1].strip())))

    # union the two sets and group by key so we have unique keys now.
    total_tweets_rdd = tuple_csvtweets_rdd.distinct().union(tup_rdd.distinct()).groupByKey()
    result_rdd = total_tweets_rdd.map(reduce_func)
    print "RESULT RDD is " + str(result_rdd.take(3))
    tuple_csvtweets_rdd.distinct().coalesce(1).saveAsTextFile('output/accuracy')
    print "THE ACCURACY IS %%%%%%%%%%%%%%%%%%%%%%%%%%%% TOTAL : "+ str(total_accm.value) +" AND CORRECT : "+ str(correct_accm.value) + " and ACCURACY IS : "+ str((float(correct_accm.value)/float(total_accm.value))*100)

    sc.stop()
