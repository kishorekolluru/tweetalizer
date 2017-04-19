import sys

import pyspark as ps

from pyspark import SparkContext


def build_word_counts(sc):
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

    def line_mapper(line):
        tokens = line.split()
        word_count_list = []
        for token in tokens:
            word_count_list.append((token, 1))
        return word_count_list

    # word_count_rdd contains <word, count> after the below step is executed
    pos_word_count_rdd = pos_uniq_tweet_rdd.flatMap(lambda line: line_mapper(line)). \
        reduceByKey(lambda val1, val2: (val1 + val2), numPartitions=1)

    neg_word_count_rdd = neg_uniq_tweet_rdd.flatMap(lambda line: line_mapper(line)). \
        reduceByKey(lambda val1, val2: (val1 + val2), numPartitions=1)

    return [pos_word_count_rdd, neg_word_count_rdd]
    # pos_word_count_rdd.saveAsPickleFile(output_file_path.split(":")[0])
    # neg_word_count_rdd.saveAsPickleFile(output_file_path.split(":")[1])
    # word_count_rdd.saveAsHadoopFile(output_file_path, 'org.apache.hadoop.mapred.TextOutputFormat')


hadoop_keyvalue_format = 'org.apache.hadoop.io.Text'
hadoop_valuevalue_format = 'org.apache.hadoop.io.IntWritable'
hadoop_textinput_format = 'org.apache.hadoop.mapred.TextInputFormat'


def concat_word_count_lists(sc, rdd_list):
    # pos_wordct_rdd = sc.pickleFile(input_file_path.split(":")[0])#, hadoop_textinput_format,
    # hadoop_keyvalue_format, hadoop_valuevalue_format)
    pos_wordct_rdd = rdd_list[0]
    # neg_wordct_rdd = sc.pickleFile(input_file_path.split(":")[1])#, hadoop_textinput_format,
    # hadoop_keyvalue_format, hadoop_valuevalue_format)
    neg_wordct_rdd = rdd_list[1]
    # print "FIRST ELEMEMTN IS .........." + str(pos_wordct_rdd.first())
    # print "TYPE ELEMEMTN IS .........." + str(type(pos_wordct_rdd.first()))
    neg_zeroed_rdd = neg_wordct_rdd.mapValues(lambda val: 0)
    pos_zeroed_rdd = pos_wordct_rdd.mapValues(lambda val: 0)
    print
    "NEGATIVE FIRST  ZEROED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + str(neg_zeroed_rdd.first())
    pos_unioned_rdd = sc.union([pos_wordct_rdd, neg_zeroed_rdd])
    neg_unioned_rdd = sc.union([neg_wordct_rdd, pos_zeroed_rdd])
    print
    "NEGATIVE FIRST UNIONED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + str(neg_unioned_rdd.first())

    pos_smoothed_rdd = pos_unioned_rdd.reduceByKey(lambda a, b: a + b, numPartitions=1)
    neg_smoothed_rdd = neg_unioned_rdd.reduceByKey(lambda a, b: a + b, numPartitions=1)
    print
    "NEGATIVE FIRST SMOOTHED %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%" + str(neg_smoothed_rdd.first())

    # print " %%%%%%%%%%%%%%%%%%%"+str(pos_smoothed_rdd.first())+"%%%%%%%%%%%%%%%%%%%%%"
    # print " %%%%%%%%%%%%%%%%%%%"+str(type(pos_smoothed_rdd.first()))+"%%%%%%%%%%%%%%%%%%%%%"

    pos_smoothed_rdd.saveAsTextFile(output_file_path.split(":")[0])  # , 'org.apache.hadoop.mapred.TextOutputFormat')
    neg_smoothed_rdd.saveAsTextFile(output_file_path.split(":")[1])  # , 'org.apache.hadoop.mapred.TextOutputFormat')


if __name__ == "__main__":
    input_file_path = "input/project/negative_bare_tweets.txt"
    output_file_path = "output/project/word_count_op/neg_output"
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    sc = SparkContext(appName="TweetAggregator")
    rdd_list = build_word_counts(sc)
    concat_word_count_lists(sc, rdd_list)
    sc.stop()
