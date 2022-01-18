from builtins import EncodingWarning
import tweepy
import json
import pymongo

# Credenciais para utilização do API do Twitter

API_key = ""
API_Secret_Key = ""
Access_Token = ""
Access_Token_Secret = " "

# Realizar autenticação no Twitter

auth = tweepy.OAuthHandler(API_key, API_Secret_Key)
auth.set_access_token (Access_Token, Access_Token_Secret)

# Construct the API instance
api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True,
                retry_count=5, retry_delay=10)

#api = tweepy.API(auth)

#Definir que palavra deseja pesquisar no Twitter

keyword = ('#openbanking OR #remediation OR #devops OR #sre OR #microservices OR #observability OR #oauth OR #metrics OR #logmonitoring OR #opentracing')

# Definir listas de armazenamento

tweets = []
info = []

file = open("tweets_keyword.txt", "a", -1, "utf-8") #salvar no diretório de trabalho

for tweet in tweepy.Cursor (api.search, 
                            q=keyword, tweet_mode='extended', 
                            rpp=100,  
                            result_type="mixed",lang='pt').itens (100):

    if 'retweeted_status' in dir(tweet): #check if retweet
        aux=tweet.retweeted_status.full_text    
    else: #not a retweet
        aux=tweet.full_text
    
    newtweet = aux.replace("\n"," ")

    tweets.append(newtweet)
    info.append(tweet)

    file.write(newtweet+'\n') #grava o texto do tweet no arquivo TXT

file.close()

# Para verificar a quantidade de tweets coletado

print("Total de tweets coletados %s." % (len(tweets)))


with open('tweets_keywords.json', 'a', encoding='utf8') as file:

    for tweet in info: 

        status = tweet

        #converte para string 
        json_str = json.dumps(status._json)

        #deserializa a string para um objeto python do tipo dict
        parsed = json.dumps(json_str)

        #grava o tweet deserializado no arquivo
        json.dump(parsed, file, ensure_ascii=False, sort_keys=True, indent=4, separators=(',')