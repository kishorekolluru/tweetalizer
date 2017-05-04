# Group 8
# Nanda Kishore Kolluru - 800970494
# Jay Shah - 800326050
# This file serves to help with the Bayes Classifier training.
import sys
import math as m

import pyspark as ps

from pyspark import SparkContext


# COmputes the bayes probabilties for the positive and negative training sets
def compute_bayes_probabilities(sc, rdd_list):
    pos_wordct_rdd = rdd_list[0]
    # neg_wordct_rdd = sc.pickleFile(input_file_path.split(":")[1])#, hadoop_textinput_format,
    # hadoop_keyvalue_format, hadoop_valuevalue_format)
    neg_wordct_rdd = rdd_list[1]
    # print "FIRST ELEMEMTN IS .........." + str(pos_wordct_rdd.first())
    # print "TYPE ELEMEMTN IS .........." + str(type(pos_wordct_rdd.first()))

    # combine positive and negative word lists with the "0" word lists of their counterparts
    neg_zeroed_rdd = neg_wordct_rdd.mapValues(lambda val: 0)
    pos_zeroed_rdd = pos_wordct_rdd.mapValues(lambda val: 0)
    print "NEGATIVE FIRST  ZEROED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + str(neg_zeroed_rdd.first())
    # Append pos and neg word lists with their counterparts' "zeroed" values
    pos_unioned_rdd = sc.union([pos_wordct_rdd, neg_zeroed_rdd])
    neg_unioned_rdd = sc.union([neg_wordct_rdd, pos_zeroed_rdd])
    print "NEGATIVE FIRST UNIONED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + str(neg_unioned_rdd.first())

    # Throw away any word same as the one in the main word list. The counterpart words have a 0 as the count
    # so reducing is gonna combine the two keys to the effect of "throwing" away duplicates
    pos_uniq_combined_rdd = pos_unioned_rdd.reduceByKey(lambda a, b: a + b, numPartitions=1)
    neg_uniq_combined_rdd = neg_unioned_rdd.reduceByKey(lambda a, b: a + b, numPartitions=1)
    print
    "NEGATIVE FIRST SMOOTHED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + str(neg_uniq_combined_rdd.first())

    # increment the combined lists by 1 to support smoothing of naive Bayes
    inc_smooth_pos_rdd = pos_uniq_combined_rdd.mapValues(lambda val: val + 1)
    inc_smooth_neg_rdd = neg_uniq_combined_rdd.mapValues(lambda val: val + 1)

    # get the total counts for each list to use in calculating the probabilities in naive bayes
    count_negative = inc_smooth_neg_rdd.map(lambda val: ('Key', val[1])).reduceByKey(lambda a, b: a + b).collect()[0]
    count_positive = inc_smooth_pos_rdd.map(lambda val: ('Key', val[1])).reduceByKey(lambda a, b: a + b).collect()[0]

    # create the naive bays probability values for each term in the list
    naive_bayes_neg = inc_smooth_neg_rdd.mapValues(lambda val: float(val) / count_negative[1])
    naive_bayes_pos = inc_smooth_pos_rdd.mapValues(lambda val: float(val) / count_positive[1])

    # naive_bayes_pos.saveAsTextFile(output_file_path.split(":")[0])  # , 'org.apache.hadoop.mapred.TextOutputFormat')
    # naive_bayes_neg.saveAsTextFile(output_file_path.split(":")[1])  # , 'org.apache.hadoop.mapred.TextOutputFormat')
    return [count_positive, count_negative, naive_bayes_pos, naive_bayes_neg]

# build the (word, totalcounts) for both positive and negative training sets
def build_word_counts(sc, input_file_path):
    pos_tweet_file_rdd = sc.textFile(input_file_path.split(":")[0])
    neg_tweet_file_rdd = sc.textFile(input_file_path.split(":")[1])
    # make the list unique
    pos_lines = pos_tweet_file_rdd.collect()
    pos_tweet_set = set(pos_lines.__iter__())
    pos_tweet_uniq_list = list(pos_tweet_set.__iter__())

    neg_lines = neg_tweet_file_rdd.collect()
    neg_tweet_set = set(neg_lines.__iter__())
    neg_tweet_uniq_list = list(neg_tweet_set.__iter__())

    # begin word count
    pos_uniq_tweet_rdd = sc.parallelize(pos_tweet_uniq_list)
    neg_uniq_tweet_rdd = sc.parallelize(neg_tweet_uniq_list)

    # split the lines and emit (word,1)
    def line_mapper(line):
        tokens = line.split()
        word_count_list = []
        for token in tokens:
            word_count_list.append((token, 1))
        return word_count_list

    # compute <word, totalcount> in below steps for +ve and -ve classes of training tweets
    pos_word_count_rdd = pos_uniq_tweet_rdd.flatMap(lambda line: line_mapper(line)). \
        reduceByKey(lambda val1, val2: (val1 + val2), numPartitions=1)

    neg_word_count_rdd = neg_uniq_tweet_rdd.flatMap(lambda line: line_mapper(line)). \
        reduceByKey(lambda val1, val2: (val1 + val2), numPartitions=1)

    return [pos_word_count_rdd, neg_word_count_rdd]


if __name__ == "__main__":
    input_file_path = "input/project/negative_bare_tweets.txt"
    output_file_path = "output/project/word_count_op/neg_output"
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    sc = SparkContext(appName="TweetAggregator")
    rdd_list = build_word_counts(sc,input_file_path)
    compute_bayes_probabilities(sc, rdd_list)
    sc.stop()
