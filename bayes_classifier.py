import sys

import pyspark as ps

from pyspark import SparkContext

if __name__ == "__main__":

    input_file_path = "input/project/negative_bare_tweets.txt"
    output_file_path = "output/project/word_count_op/neg_output"
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]

    sc = SparkContext(appName="TweetAggregator")
    tweet_file_rdd = sc.textFile(input_file_path)
    # make the list unique
    lines = tweet_file_rdd.collect()
    tweet_set = set(lines.__iter__())
    tweet_uniq_list = list(tweet_set.__iter__())

    # begin word count
    uniq_tweet_rdd = sc.parallelize(tweet_uniq_list)


    def line_mapper(line):
        tokens = line.split()
        word_count_list = []
        for token in tokens:
            word_count_list.append((token, 1))
        return word_count_list


    # word_count_rdd contains <word, count> after the below step is executed
    word_count_rdd = uniq_tweet_rdd.flatMap(lambda line: line_mapper(line)). \
        reduceByKey(lambda val1, val2: (val1 + val2), numPartitions=1)

    word_count_rdd.saveAsHadoopFile(output_file_path,'org.apache.hadoop.mapred.TextOutputFormat')
    sc.stop()
