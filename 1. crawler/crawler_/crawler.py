from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream


#Variables that contains the user credentials to access Twitter API 
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


#This is a basic listener that just prints received tweets.
class MyListener(StreamListener):   
    def on_data(self, data):
        try:
            with open('C:\Baturu\data007.json', 'a') as f:
                f.write(data)
                f.close()
                return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        #print(data)  
        return True
       

    def on_error(self, status):
        print (status)


if __name__ == '__main__':


    l = MyListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
#    stream.filter( languages=['en'] )
    #This line filter Twitter Streams to capture data by the keywords: 'python','java','C++','ruby','news' and
#    stream.filter(locations = [-180, -90, 180, 90])
    stream.filter(languages=['en'], locations = [-125, 25, -73, 49, -170, 60, -140, 70])
    #stream.filter(languages=['en'],locations = [-170, 60, -140, 70])