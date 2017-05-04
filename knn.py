# Group 8
# Nanda Kishore Kolluru - 800970494
# Jay Shah - 800326050
# This file serves as the main py file that calculates the class of a tweet based on the KNN algorithm
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


# Calculate the cosine similarity value comparing with the +ve and -ve feature vectors
def cosine(feature_vec):
    test_feat_matrix = np.array(feature_vec)
    sim_pos = float(-10000000)
    sim_neg = float(-10000000)
    n = N.value
    simi_plist = []
    simi_nlist = []
    # Calc the cosine similarity of the test tweet against the +ve and -ve feature vectors
    for val in positive_feature_bcast.value:
        pos_mat = np.array(val[1])
        x = np.dot(pos_mat, test_feat_matrix)
        temp_sim_pos = float(x) / float((pos_mat.__len__() ** 2))
        simi_plist.append(temp_sim_pos)
        simi_plist = sorted(simi_plist, reverse=True)[:n]
        if temp_sim_pos > sim_pos:
            sim_pos = temp_sim_pos

    for val in negative_feature_bcast.value:
        neg_mat = np.array(val[1])
        x = np.dot(neg_mat, test_feat_matrix)
        temp_sim_neg = float(x) / float((neg_mat.__len__() ** 2))
        simi_nlist.append(temp_sim_neg)
        simi_nlist = sorted(simi_nlist, reverse=True)[:n]
        if temp_sim_neg > sim_neg:
            sim_neg = temp_sim_neg

    pcount = 0
    ncount = 0
    # return the class with the largest number similarity of the N neighbors
    for i, j in zip(simi_plist, simi_nlist):
        if pcount + ncount < n:
            if i >= j:
                pcount += 1
            elif i < j:
                ncount += 1
        else:
            break

    if pcount > ncount:
        return 1
    elif pcount < ncount:
        return 0
    return -1


unique_dict_bcast = 0
N = 0

if __name__ == '__main__':
    sc = SparkContext(appName="KNN")
    clean_tweet_text_filename = ''

    # call the
    input_file_path = sys.argv[1]
    input_test_path = sys.argv[2]
    output_file_path = sys.argv[3]
    N = int(sys.argv[4])
    N = sc.broadcast(N)
    pos_tweets_rdd = sc.textFile(input_file_path.split(":")[0], minPartitions=10)
    neg_tweets_rdd = sc.textFile(input_file_path.split(":")[1], minPartitions=10)
    # pos_word_tweetnamesTFs_rdd = compute_word_twtnamesTFs_rdd(sc, pos_tweets_rdd)
    # neg_word_tweetnamesTFs_rdd = compute_word_twtnamesTFs_rdd(sc, neg_tweets_rdd)

    cl_pos_tweets_rdd = pos_tweets_rdd  # already cleaned
    cl_neg_tweets_rdd = neg_tweets_rdd
    pos_tweet_count_bcast = sc.broadcast(cl_pos_tweets_rdd.count())
    neg_tweet_count_bcast = sc.broadcast(cl_neg_tweets_rdd.count())

    cleaned_tweets_rdd = cl_pos_tweets_rdd.union(cl_neg_tweets_rdd)

    ######
    # braodcast the total tweet count
    print "UNIONED TWEETS TAKE3 $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(cleaned_tweets_rdd.take(3))
    total_tweets_bcast = sc.broadcast(float(cleaned_tweets_rdd.count()))
    print "Total TWEET COUNT $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(total_tweets_bcast.value)

    # #DICTIONARY OF UNIQUE +ve AND -ve WORDS
    unique_words_dict_rdd = cleaned_tweets_rdd.flatMap(lambda val: val.split()).distinct().zipWithIndex()
    unique_dict_bcast = sc.broadcast(unique_words_dict_rdd.collectAsMap())
    print "DONE WITH DICT CREATION $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(unique_dict_bcast.value)

    # training our postive and negative tweets, derive the feature vectors
    postive_tweet_features = cl_pos_tweets_rdd.map(lambda val: ("1", feature_vector_assign(val)))
    negative_tweet_features = cl_neg_tweets_rdd.map(lambda val: ("0", feature_vector_assign(val)))
    print "FEATURE VECTORS POSITIVE TAKE10$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(postive_tweet_features.take(10))

    positive_feature_bcast = sc.broadcast(postive_tweet_features.collect())
    negative_feature_bcast = sc.broadcast(negative_tweet_features.collect())
    # print "FEATURES VECTORS BROADCAST $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(positive_feature_bcast.value)
    # clean the testing tweets and extract feature vectors
    test_tweets = sc.textFile(input_test_path)
    cl_test_tweets_rdd = test_tweets.map(clean_tweet)

    # have a reference of the cleaned tweets against the original tweets
    clean_and_original_rdd = cl_test_tweets_rdd.zip(test_tweets).coalesce(1)
    # zip_rdd.saveAsTextFile("output/zip")
    zip_bcast = sc.broadcast(clean_and_original_rdd.collectAsMap())

    test_features_rdd = cl_test_tweets_rdd.map(lambda val: (val, feature_vector_assign(val)))
    print " TESTING TWEETS FEATURES TAKE10 $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(test_features_rdd.take(10))
    # calc the cosine similarity for the test tweets
    classified_tweets_rdd = test_features_rdd.mapValues(cosine)
    print "THE CLASSIFIED TWEETS TAKE100$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" + str(classified_tweets_rdd.take(100))

    # classified_tweets_rdd.saveAsTextFile("output/knn1")
    # replace the cleaned tweets with the original uncleaned tweets
    classified_rtweets_rdd = classified_tweets_rdd.map(lambda kv: (zip_bcast.value[kv[0]], kv[1]))
    classified_rtweets_rdd.saveAsTextFile(output_file_path)
    sc.stop()
