import json
from http.client import IncompleteRead
from io import BufferedWriter
from json import JSONEncoder
import tweepy
import _datetime

from pip._vendor.requests.packages.urllib3.exceptions import ProtocolError

twitter_auth = tweepy.OAuthHandler("Eug8CUA36W1QSxQiARfVR9jAQ", "p24M9jwx9BGLKCi5yxGbIKzBG4sCtmVpO5RzNzmQiCa60dd0KR")
twitter_auth.set_access_token("847427862021812224-TPX5Tyq0EsLIkKU3nDyhSzsALejM6bv",
                              "WPkWhmY2E8Fyf71DJOih4KNkvQjuX1HvlaFtxxyt5l0K9")

twitter_api = tweepy.API(twitter_auth)

#remove hastags, mentions, RT, unicode chars, links, stop words

positiveTrackList = [
    u'\U0000263A',
    u'\U0001F600',
    u'\U0001F601',
    u'\U0001F602',
    u'\U0001F603',
    u'\U0001F604',
    u'\U0001F605',
    u'\U0001F606',
    u'\U0001F607',
    u'\U0001F608',  # devilish smile
    u'\U0001F609',
    u'\U0001F60A',
    u'\U0001F60B',
    u'\U0001F60C',
    u'\U0001F60D',
    u'\U0001F60E',
    u'\U0001F60F',  # smirky face
    u'\U0001F923',  # rofl
    u'\U0001F92D',
    u'\U0001F92E',
    u'\U0001F913',  # geeky smile
    u'\U0001F920',  # cowboy hat face
    u'\U0001F921',  # clown
    u'\U0001F917',  # huggy face

    u'\U0001F642',
    u'\U0001F917',
    u'\U0001F60F',
    u'\U0001F61B',
    u'\U0001F61C',
    u'\U0001F61D',
    u'\U0001F643',  # upside down face
    u'\U0001F924',  # Drool
    u'\U0001F47B',  # Ghosty tongue out

    u'\U0001F4A9',  # Shit
    # Cat emojis
    u'\U0001F63A',
    u'\U0001F638',
    u'\U0001F639',
    u'\U0001F63B',
    u'\U0001F63C',
    u'\U0001F63D',
    # Monkey faces
    u'\U0001F648',
    u'\U0001F649',
    u'\U0001F64A',
    # Hand signs
    u'\U0001F44F',  # Applause
    u'\U0001F64C',  # hi fi
    u'\U0001F91D',  # Hand shake

    u'\U0001F48B',  # Lipstick lips
    u'\U0001F498',  # Cupid struck
    u'\U00002764',  # Heart
    u'\U0001F495',  # Hearts
    u'\U0001F496',  # Hearts
    u'\U0001F49C',  # Hearts
    u'\U0001F48C',  # Hearts
    u'\U0001F436',  # Pup
    u'\U0001F31E',  # Happy Sun
    u'\U0001F31D',  # Happy Moon
    u'\U00002B50',  # Star
    u'\U0001F383',  # Halloween pumpkin
    u'\U0001F389',  # Party snouts
    u'\U0001F38A',  # Party confetti
    u'\U0001F381',  # Gift
    u'\U0001F3C6',  # Winner Cup

]


class TwitterInfo:
    def __init__(self, created_at, id_str, text, source, user_location, user_id, user_name, user_lang,
                 geo, coordinates, place, hashtags, lang, filter_level, timestamp_ms):
        self.created_at = created_at
        self.id_str = id_str
        self.text = text
        self.source = source
        self.user_location = user_location
        self.user_id = user_id
        self.user_name = user_name
        self.user_lang = user_lang
        self.geo = geo
        self.coordinates = coordinates
        self.place = place
        self.hashtags = hashtags
        self.lang = lang
        self.filter_level = filter_level
        self.timestamp_ms = timestamp_ms


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, file_handle):
        self.file_handle = file_handle
        self.counter = 0
        print('{\n"twitterdata" : [', file=file_handle, end='\n')

    def on_status(self, status):
        # print("######### New Tweet########: {}".format(status.text))
        pass

    def on_data(self, raw_data):
        self.counter += 1
        print("Tweet {} received".format(self.counter))
        json_data = json.loads(raw_data)
        #        print(raw_data)
        if 'created_at' in json_data:
            printed_json = json.dumps(TwitterInfo(json_data["created_at"],
                                                  json_data["id_str"],
                                                  json_data["text"],
                                                  json_data["source"],
                                                  json_data["user"]['location'],
                                                  json_data["user"]['id_str'],
                                                  json_data["user"]['name'],
                                                  json_data["user"]['lang'],
                                                  json_data["geo"],
                                                  json_data["coordinates"],
                                                  json_data["place"],
                                                  json_data["entities"]["hashtags"],
                                                  json_data["lang"],
                                                  json_data["filter_level"],
                                                  json_data["timestamp_ms"]).__dict__)
            print(printed_json + '\n,', file=self.file_handle, end='\n')

    def on_disconnect(self, notice):
        print("Disconnecting...{}".format(notice))

    def on_error(self, status_code):
        if status_code == 420:
            print("Error code 420! Stopping streaming!!!!!!!")
            return False
        elif status_code == 123:
            print("You interrupted!!")
            return False


def main():
    outfile = open('dummy.json', mode='w', buffering=800000)

    myStreamListener = MyStreamListener(outfile)
    try:
        myStream = tweepy.Stream(auth=twitter_api.auth, listener=myStreamListener)
        myStream.filter(track=positiveTrackList, async=True, languages=['en'])
    except (IOError, TypeError, ProtocolError, IncompleteRead) as e:
        print("EXCEPTION!!!!  {}".format(e))

    cmd = 'a'
    while cmd != 's':
        cmd = input("COMMAND: ")

    myStream.disconnect()
    print('"end": "true"\n]\n}', file=outfile, end='\n')
    outfile.close()


if __name__=="__main__": main()
