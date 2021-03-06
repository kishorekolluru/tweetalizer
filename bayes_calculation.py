import re
import sys

from bayes_classifier import build_word_counts, compute_bayes_probabilities
from pyspark import SparkContext
from nltk.corpus import stopwords
import math

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

    tweet_text = slash_stop_words(tweet_text)# remove stop words
    return tweet_text

def slash_stop_words(tweet_text):
        tweet_line_clean = ''
        for word in tweet_text.lower().split():
            if word not in stop_word_list:
                tweet_line_clean += word + ' '
        return tweet_line_clean if tweet_line_clean != '' else ''

# distributed method
def find_class_probability(current_dict, tweet, original_count):
    final_probability = 1
    num_not_in_curr_dict = 0
    cleaned_tweet = clean_tweet(tweet)
    word_list = cleaned_tweet.split(' ')
    if len(word_list)>0:
        for w in word_list:
            if w not in current_dict:
                num_not_in_curr_dict = num_not_in_curr_dict + 1

        if num_not_in_curr_dict == 0:
            for w in word_list:
                final_probability = current_dict[w] * final_probability
        else:
            new_count = original_count + num_not_in_curr_dict
            for w in word_list:  # per tweet
                if w in current_dict:
                    new_prob_word = (current_dict[w] * original_count) / (new_count)
                    final_probability = final_probability * new_prob_word
                    # for each word_list, gra the value from current dictionary and emit
                else:
                    temp_probability = float(1) / (new_count)
                    final_probability = final_probability * temp_probability
    else:
        return 0
    return final_probability

# dist method
def determine_pos_neg(combined_prob):
    positive_prob = float(combined_prob.split(combined_prob_delimiter)[0])
    neg_prob = float(combined_prob.split(combined_prob_delimiter)[1])
    if positive_prob > neg_prob:
        return 1
    elif positive_prob < neg_prob:
        return 0
    return -1  # neither +ve nor -ve


combined_prob_delimiter = ":::::"
if __name__ == '__main__':
    sc = SparkContext(appName="TweetAggregator")
    clean_tweet_text_filename = 'trump_stopwordless_tweets.txt'

    # call the
    input_file_path = sys.argv[1]
    input_tweet_file = sys.argv[2]
    output_file_path = sys.argv[3]

    rdd_list = build_word_counts(sc, input_file_path)
    pos_neg_naive_probs_rdd_list = compute_bayes_probabilities(sc, rdd_list)

    pos_orig_count = pos_neg_naive_probs_rdd_list[0][1]
    neg_orig_count = pos_neg_naive_probs_rdd_list[1][1]
    pos_dict = pos_neg_naive_probs_rdd_list[2].collectAsMap()
    neg_dict = pos_neg_naive_probs_rdd_list[3].collectAsMap()
    print "Total "
    print "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% NAIVE PROBABILITIES COLLECTED AS MAPS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"

    trump_tweets_rdd = sc.textFile(input_tweet_file)

    pos_tweet_probabilites_rdd = trump_tweets_rdd.map(
        lambda line: (line, find_class_probability(pos_dict, line, pos_orig_count)))

    neg_tweet_probabilites_rdd = trump_tweets_rdd.map(
        lambda line: (line, find_class_probability(neg_dict, line, neg_orig_count)))

    print "%%%%%%%%%%%%TWEET PROBAB %%%%%%%%%%%%" + str(pos_tweet_probabilites_rdd.take(5))

    both_pos_neg_probs = pos_tweet_probabilites_rdd.union(neg_tweet_probabilites_rdd)
    combined_probs_rdd = both_pos_neg_probs.reduceByKey(lambda a, b: str(a) + combined_prob_delimiter + str(b))

    print "%%%%%%%%%%%%CIMBINED PROBAB %%%%%%%%%%%%" + str(combined_probs_rdd.take(5))

    combined_probs_rdd.map(lambda tuple: (tuple[0], determine_pos_neg(tuple[1]))) \
        .saveAsTextFile(output_file_path)

    sc.stop()
