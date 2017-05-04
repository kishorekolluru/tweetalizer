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


def emit_word_tweet_count_1(twtPlusFile):
    tweet_line = twtPlusFile[0]
    tweet_file = twtPlusFile[1]

    words = tweet_line.split()
    emit_list = []
    for word in words:
        emit_list.append((word + in_tweet_delim + tweet_file, 1))
    return emit_list


def emit_word_tweetTf(tuple):
    key = tuple[0]
    tweet_tweetName = key.split(in_tweet_delim)
    # set key to tweet
    key = tweet_tweetName[0]
    # retrieve TF
    val = tuple[1]
    # combine tweetname and TF
    val = tweet_tweetName[1] + in_tweet_delim + str(val)

    return (key, val)


def calc_tfidf(tuple):
    word = tuple[0]
    tweetnames_tfs = tuple[1]

    tweetnames_tfidfs_list = tweetnames_tfs.split(inter_tweet_delim)
    num_files_relevant = float(len(tweetnames_tfidfs_list))

    idf_val = float(math.log10(1 + (total_tweets_bcast.value) / num_files_relevant))

    emit_list = []
    for twtname_tf_val in tweetnames_tfidfs_list:
        twt_name = twtname_tf_val.split(in_tweet_delim)[0]
        tf_val = float(twtname_tf_val.split(in_tweet_delim)[1])
        tfidf = tf_val * idf_val
        emit_list.append((word + in_tweet_delim + twt_name, tfidf))
    return emit_list


def emit_tweetName_wordTfIdf(tuple):
    array = tuple[0].split(in_tweet_delim)
    tf_idf_val = tuple[1]

    word = array[0]
    tweet_name = array[1]
    wordTfidf = word + in_tweet_delim + str(tf_idf_val)
    return (tweet_name, wordTfidf)


def create_design_mat(tuple):
    twtname = tuple[0]
    word_tfs_str = tuple[1]
    i = 0
    twt_word_ind_dict = {}

    for spl in word_tfs_str.split(inter_tweet_delim):
        word = spl.split(in_tweet_delim)[0]
        tfidf = float(spl.split(in_tweet_delim)[1].strip())
        ind = word_index_dict_bcast.value[word]
        twt_word_ind_dict[ind] = tfidf
    design_row = []
    while i < total_tweet_words_bcast.value:
        design_row.append(twt_word_ind_dict.get(i, float(0)))
        i = i + 1
    sparse_des_row = Matrices.dense(1, total_tweet_words_bcast.value,design_row).toSparse()
    # return the float design matrix rw for tweetname
    return (twtname, sparse_des_row)

def zip_with_y(tuple):
    twtName = tuple[0]
    tweetNum = int(twtName.split("_")[1])
    if tweetNum < pos_tweet_count_bcast:
        return (twtName + "##"+str(1), tuple[1])
    else:
        return (twtName + "##"+str(0), tuple[1])

def calc_betaj(tuple):
    twtnameY = tuple[0]
    y = int(twtnameY.split("##")[1])

    hb = 1/(1+math.exp())
    yhb = y -

if __name__ == '__main__':
    sc = SparkContext(appName="Logistic Regression")
    clean_tweet_text_filename = ''

    # call the
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    pos_tweets_rdd = sc.textFile(input_file_path.split(":")[0],minPartitions=10)
    neg_tweets_rdd = sc.textFile(input_file_path.split(":")[1], minPartitions=10)
    # pos_word_tweetnamesTFs_rdd = compute_word_twtnamesTFs_rdd(sc, pos_tweets_rdd)
    # neg_word_tweetnamesTFs_rdd = compute_word_twtnamesTFs_rdd(sc, neg_tweets_rdd)

    # clean the tweets of the nonsense
    cl_pos_tweets_rdd = pos_tweets_rdd.map(clean_tweet)
    cl_neg_tweets_rdd = neg_tweets_rdd.map(clean_tweet)
    cleaned_tweets_rdd = cl_pos_tweets_rdd.union(cl_neg_tweets_rdd)

    pos_tweet_count_bcast = sc.broadcast(cl_pos_tweets_rdd.count())
    neg_tweet_count_bcast = sc.broadcast(cl_neg_tweets_rdd.count())

    ######
    # braodcast the total tweet count
    print "CLEANED TWEETS ::::::::::" + str(cleaned_tweets_rdd.take(3))
    total_tweets_bcast = sc.broadcast(float(cleaned_tweets_rdd.count()))
    print "Total TWEETS ::::::::::  " + str(total_tweets_bcast.value)
    # create "file" names for the tweets
    zipped_clean_tweets_rdd = cleaned_tweets_rdd.zipWithIndex()
    tweets_filed_rdd = zipped_clean_tweets_rdd.map(lambda tuple: (tuple[0], "tweet_" + str(tuple[1])))
    ########
    # get RDD of <"word %%% tweetname, 1> pairs
    print "Tweets with FILE NAMES%%%%%%%%%%%%%%%%%%%%%" + str(tweets_filed_rdd.take(3))
    wordTweetname_one_rdd = tweets_filed_rdd.flatMap(lambda twt_twtName: emit_word_tweet_count_1(twt_twtName))
    # reduce the 1s to the sum and take the log10 of the value. Of the form <"word%%%tweetname, 1+log10(word sum)>
    wordTweetname_TF_rdd = wordTweetname_one_rdd.reduceByKey(lambda a, b: a + b).mapValues(
        lambda val: 1 + math.log10(val))

    # emit <word, tweetname ## TF> pairs
    word_tweetnameTF_rdd = wordTweetname_TF_rdd.map(emit_word_tweetTf)
    # reduce on word into <word, tweetname##Tf$$tweetname2##TF2...>
    word_tweetnamesTFs_rdd = word_tweetnameTF_rdd.reduceByKey(lambda a, b: a + inter_tweet_delim + b)
    word_tweetnamesTFs_rdd.persist()
    print "WORD %%% TWEETNAMES TFSS %%%%%%%%%%%%%%%%" + str(word_tweetnamesTFs_rdd.take(3))

    # create the VOCAB DICT with ['word', index]
    word_dict_rdd = word_tweetnamesTFs_rdd.keys().zipWithIndex()
    # print "THE VOCAB DICT SORTED ON VALUE %%%%%%%%%%%%%%%%%%%%" + str(word_dict_rdd.map(lambda kv : (kv[1], kv[0])).sortByKey().collectAsMap())
    word_index_dict_bcast = sc.broadcast(word_dict_rdd.collectAsMap())
    # print "THE VOCAB DICT %%%%%%%%%%%%%%%%%%%%" + str(word_index_dict_bcast.value)
    # get the total tweet words length
    total_tweet_words_bcast = sc.broadcast(len(word_index_dict_bcast.value))

    # CALCULATE TFIDF and emit <word # tweetname, tfidf> pairs
    wordTwtName_tfidf_rdd = word_tweetnamesTFs_rdd.flatMap(calc_tfidf)
    print "WORD_TWTNAME AND TFIDF VALS %%%%%%%%%%%%%%%%%%%%" + str(wordTwtName_tfidf_rdd.take(3))
    # emit <tweetname, word%==%tfidf %%^^%% word%==%tfidf....> pairs
    twtName_wordTfidfs_rdd = wordTwtName_tfidf_rdd.map(emit_tweetName_wordTfIdf).reduceByKey(
        lambda a, b: a + inter_tweet_delim + b)
    print "TWTNAME AND Word_TFIDF VALS %%%%%%%%%%%%%%%%%%%%" + str(twtName_wordTfidfs_rdd.take(3))
    # create the design matrix
    design_matrix_rdd = twtName_wordTfidfs_rdd.map(create_design_mat)
    print "DESIGN MATRIX MANNNNN YOU MADE IT %%%%%%%%%%%%%%%%%%%%%%" + str(design_matrix_rdd.take(3))
    # wordTwtName_tfidf_rdd.unpersist()
    print "DESIGN MAT RDD HAS PARTITIONS %%%%%%%%%%%%%%%%%%%%%%%" + str(design_matrix_rdd.getNumPartitions())
    # design_matrix_rdd.saveAsTextFile(output_file_path)
    design_matrix_y_rdd = design_matrix_rdd.map(zip_with_y)

    design_matrix_y_rdd.map(calc_betaj)
    j=0
    alpha = 0.0001
    beta = np.ones((total_tweet_words_bcast.value, 1))
    while j < total_tweet_words_bcast.value:
        m = 0, yhb = 0
        minusBetatran_X = 0
        yhb = 1/(1+math.exp(minusBetatran_X))
        while m< total_tweets_bcast:
            # yhb =
        beta[j] = beta[j] + alpha ()

    total_tweets_bcast.unpersist()
    word_index_dict_bcast.unpersist()
    total_tweet_words_bcast.unpersist()

    # ones_rdd = sc.range(1, end=pos_tweets_rdd.count()).map(lambda val: 1)
    # zeros_rdd = sc.range(1, end=neg_tweets_rdd.count()).map(lambda val: 0)
    # pos_design_rdd.zip(ones_rdd)
    # neg_design_rdd.zip(zeros_rdd)
    # # now we have the
    # design_rdd = pos_design_rdd.union(neg_design_rdd)

    sc.stop()

