import sys

import math
import numpy as np
import scipy as sp
import re

from pyspark.mllib.linalg import Matrix, Matrices
from pyspark import SparkContext
from nltk.corpus import stopwords

in_tweet_delim = '%==%'
inter_tweet_delim = '%%^^%%'

total_tweets_bcast = 0
stop_word_list = set(stopwords.words('english'))


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
    tweet_text = tweet_text.lower().strip()

    tweet_text = slash_stop_words(tweet_text)  # remove stop words
    return tweet_text


def slash_stop_words(tweet_text):
    tweet_line_clean = ''
    for word in tweet_text.lower().split():
        if word not in stop_word_list:
            tweet_line_clean += word + ' '
    return tweet_line_clean if tweet_line_clean != '' else ''

def feature_vector_assign(line):
    tokens = line.split()
    # getting length of dictionary to initialize
    dict_length = len(unique_dict_bcast.value)
    feature_vector = [0] * dict_length

    for t in tokens:
        if t in unique_dict_bcast.value:
            feature_vector[unique_dict_bcast.value[t]] = 1

    return feature_vector


def cosine(tline):
    feature_vec = tline[1]
    test_feat_matrix = np.array(feature_vec)
    sim_pos = float(-10000000)
    sim_neg = float(-10000000)

    for val in positive_feature_bcast.value:
        pos_mat = np.array(val[1])
        x = np.dot(pos_mat, test_feat_matrix)
        temp_sim_pos = float(x) / float((pos_mat.__len__() ** 2))
        if temp_sim_pos > sim_pos:
            sim_pos = temp_sim_pos

    for val in negative_feature_bcast.value:
        neg_mat = np.array(val[1])
        x = np.dot(neg_mat, test_feat_matrix)
        temp_sim_neg = float(x) / float((neg_mat.__len__() ** 2))
        if temp_sim_neg > sim_neg:
            sim_neg = temp_sim_neg

    if sim_pos > sim_neg:
        return (sim_pos, 1)
    elif sim_pos < sim_neg:
        return (sim_neg, 0)
    return (-1, -1)

unique_dict_bcast = 0

if __name__ == '__main__':
    sc = SparkContext(appName="KNN")
    clean_tweet_text_filename = ''

    # call the
    input_file_path = sys.argv[1]
    input_test_path = sys.argv[2]
    output_file_path = sys.argv[3]

    pos_tweets_rdd = sc.textFile(input_file_path.split(":")[0], minPartitions=10)
    neg_tweets_rdd = sc.textFile(input_file_path.split(":")[1], minPartitions=10)
    # pos_word_tweetnamesTFs_rdd = compute_word_twtnamesTFs_rdd(sc, pos_tweets_rdd)
    # neg_word_tweetnamesTFs_rdd = compute_word_twtnamesTFs_rdd(sc, neg_tweets_rdd)

    # clean the tweets of the nonsense
    cl_pos_tweets_rdd = pos_tweets_rdd
    cl_neg_tweets_rdd = neg_tweets_rdd
    pos_tweet_count_bcast = sc.broadcast(cl_pos_tweets_rdd.count())
    neg_tweet_count_bcast = sc.broadcast(cl_neg_tweets_rdd.count())

    cleaned_tweets_rdd = cl_pos_tweets_rdd.union(cl_neg_tweets_rdd)


    ######
    # braodcast the total tweet count
    print "UNIONED TWEETS $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(cleaned_tweets_rdd.take(3))
    total_tweets_bcast = sc.broadcast(float(cleaned_tweets_rdd.count()))
    print "Total TWEET COUNT $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(total_tweets_bcast.value)

    # #DICTIONARY OF UNIQUE +ve AND -ve WORDS
    unique_words_dict_rdd = cleaned_tweets_rdd.flatMap(lambda val: val.split()).distinct().zipWithIndex()
    unique_dict_bcast = sc.broadcast(unique_words_dict_rdd.collectAsMap())
    print "THE DICT TYPE IS $$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(type(unique_dict_bcast.value))
    print "DONE WITH DICT CREATION $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(unique_dict_bcast.value)

    # training our postive and negative tweets, derive the feature vectors
    postive_tweet_features = cl_pos_tweets_rdd.map(lambda val: ("1", feature_vector_assign(val)))
    negative_tweet_features = cl_neg_tweets_rdd.map(lambda val: ("0", feature_vector_assign(val)))
    print "FEATURE VECTORS POSITIVE $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(postive_tweet_features.take(100000))

    positive_feature_bcast = sc.broadcast(postive_tweet_features.collect())
    negative_feature_bcast = sc.broadcast(negative_tweet_features.collect())
    print "FEATURES VECTORS BROADCAST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(positive_feature_bcast.value)
    # clean the testing tweets and extract feature vectors
    cl_test_tweets_rdd = sc.textFile(input_test_path).map(clean_tweet)
    test_features_rdd = cl_test_tweets_rdd.map(lambda val: ("test", feature_vector_assign(val)))
    print " TESTING TWEETS FEATURES $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(test_features_rdd.take(10000))

    classified_tweets = test_features_rdd.map(cosine)
    print "THE CLASSIFIED TWEETS $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(classified_tweets.take(100))
    classified_tweets.saveAsTextFile(output_file_path)
    sc.stop()
