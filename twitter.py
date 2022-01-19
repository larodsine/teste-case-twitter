from ast import In
from dataclasses import replace
from datetime import datetime
from doctest import OutputChecker
from ssl import create_default_context
from TwitterSearch import *

import json
import pandas as pd
import nltk
import re

# Credenciais de acesso

consumer_key = "wIEDxwQBT2Hs1a83MRDz2GCAe" 
consumer_secret = "6EStVWgEc1kd3P4f5iV4MaT0gcuYg7BZ8g8bhm5iiXnElGv04F"

access_token = "2638713761-YCW7Z6jpD3eZjAL7PY1kaUIjwHZ25dNg2p7hKb3"
access_token_secret = "sNKZIX8pxTVa3A6phcFFiSjSCVbJ3E10MZaD4G7aGePUy"

try :

    ts = TwitterSearch( 
        consumer_key = consumer_key,
        consumer_secret = consumer_secret,
        access_token = access_token,
        access_token_secret= access_token_secret

    )

    tso = TwitterSearchOrder() # cria objeto TwitterSearchOrder
    tso.set_keywords (['openbanking','devops','sre','remediation', 'microservices,', 'observability', 'oauth', 'metrics', 'logmonitoring', 'opentracing'], or_operator = True)
    tso.set_language ('pt')

    for tweet in ts.search_tweets_iterable(tso): # ts.search_tweets_iterable(tso) é um metadata

        print('created_at: ',tweet['created_at'], 'User_id: ', tweet['id_str'], tweet['text'])

        created_at = tweet['created_at']
        user_id = tweet['id_str']
        texto = tweet['text']

        with open("tweet.json", "a+") as output:

            data = {"created_at": created_at,
                    "user_id": user_id,
                    "texto": texto}
            
            #print(data) escrever dentro do arquivo JSON
            output.write("{}\n".format(json.dumps(data)))

except TwitterSearchException as e:
    print(e)

df = pd.read_json('tweet.json', lines=True)


df.shape

# Removendo os valores duplicados

df.drop_duplicates(['tweet'], inplace=True)

df.shape

# Função para remover stopwords da base

def RemoveStopWords (instancia):
    stopwords = set(nltk.corpus.stopwords.words('portugueses'))
    palavras = [i for i in instancia.split() if not i in stopwords]
    return (" ".join(palavras))

# Aplicando o stemming na base

def Stemming(instancia):
    stemmer = nltk.stem.RSLPStemmer()
    palavras = []
    for w in instancia.split():
        palavras.append(stemmer.stem(w))
    return (" ".join(palavras))

# Remove links, caracteres etc
'''
def Limpeza_dados(instancia):
    #remove links, pontos, virgulas 
    instancia = re.sub(r"http\S+", "", instancia).lower(),replace('.','').replace(';', '').replace('-','').replace(':', '').replace(')', '')
    return(instancia)'''

def Preprocessing(instancia):
    instancia = re.sub(r"http\S+", "", instancia).lower(),replace('.','').replace(';', '').replace('-','').replace(':', '').replace(')', '')
    stopwords = set(nltk.corpus.stopwords.words('portuguese'))
    palavras = [i for i in instancia.split() if not i in stopwords]
    return(" ".join(palavras))

tweets = [Preprocessing(i) for i in df.tweet]
tweets [:10]
df['preprocessed'] = tweets

    
from bs4 import BeautifulSoup

def tweet_to_words(tweet):

    tweet = BeautifulSoup(tweet, "html.parser").get_text() # Remove HTML tags
    tweet = re.sub(r"[^a-zA-Zà-úÀ-Ú0-9]", "", tweet.lower()) # Limpa e Converte para minúsculo
    #words = tweet_tokenizer.tokenize(tweet)

    return tweet


df['cleaned_tweets'] = [prep_tweets(tweet) for tweet in df.preprocessed]

from sklearn.feature_extraction.text import CountVectorizer

cv = CountVectorizer()
count_matrix = cv.fit_transform(df.cleaned_tweets)