# Importando as bibliotecas

from TwitterAPI import TwitterAPI
import elasticsearch
import logging
import rfc822
import datetime
import numbers
import time
from subprocess import Popen, PIPE

def patch_tweet(d):
    """A API do twitter retorna as datas num formato que o elasticsearch não consegue reconhecer,
        então precisamos parsear a data para um formato que o ES entende, essa função faz isso.
    """

    if 'created_at' in d:
        # twitter uses rfc822 style dates. elasticsearch uses iso dates.
        # we translate twitter dates into datetime instances (pyes will
        # convert datetime into the right iso format understood by ES).
        new_date = datetime.datetime(*rfc822.parsedate(d['created_at'])[:6])
        d['created_at'] = new_date

    count_is_number = isinstance(d['retweet_count'], numbers.Number)
    if 'retweet_count' in d and not count_is_number:
        # sometimes retweet_count is a string instead of a number (eg. "100+"),
        # here we transform it to a number (an attribute in ES cannot have
        # more than one type).
        d['retweet_count'] = int(d['retweet_count'].rstrip('+')) + 1

    return d

def check_es_status():
   """Essa é uma função que verifica se o serviço do ElasticSearch está operando
       e inicia-o ou reinicia-o caso seja necessário.
   """
    cmd = Popen(["service", "elasticsearch", "status"], stdout=PIPE)
    cmd_out, cmd_err = cmd.communicate()
    print(cmd_out) #print para acompanhamento no shell
    if "not running" in cmd_out:
        print "Elastic Search Not Running, trying to start it"
        cmd = Popen(["service", "elasticsearch", "start"], stdout=PIPE)
        cmd_out, cmd_err = cmd.communicate()
        print(cmd_out)  #print para acompanhamento no shell
    time.sleep(15)

# configurando traces e logs
log_dir = "/var/log/elasticsearch/"
tracer = logging.getLogger('elasticsearch.trace')
tracer.setLevel(logging.WARN)
tracer.addHandler(logging.FileHandler(log_dir + 'trace.log'))
default_logger = logging.getLogger('Elasticsearch')
default_logger.setLevel(logging.WARN)
default_logger.addHandler(logging.FileHandler(log_dir + 'default.log'))

# Criando conexão ao elasticsearch
es = elasticsearch.Elasticsearch(["<dominio_do_servidor>:9200"]) # domínio sem o http://

# Configurando as chaves de acesso à API do Twitter.
# Não esqueça de alterar os valores abaixo para os da sua conta.
twitter_api = TwitterAPI(consumer_key='consumer_key',
                      consumer_secret='consumer_secret',
                      access_token_key='access_token',
                      access_token_secret='access_token_secret')

#Aqui inicialmos os filtros que desejamos, para ver a documentação completa acesse:
# https://dev.twitter.com/docs/streaming-apis/parameters
# No caso iremos usar apenas o filtro de idioma e por "palavras" (os termos em "track")
# veja mais sobre como escolher as palavras aqui: https://dev.twitter.com/docs/streaming-apis/parameters#track
filters = {
    "language": ["pt"],
    "track": [ "software", "livre", "open", "source", "floss", "gnu", "gpl", "polignu" ]
}

#Agora é que a mágina acontece, esse é um código feito para rodar 24x7
while True:
    #creates the stream object
    stream = twitter_api.request('statuses/filter', filters)

    #For each item in the stream (tweet data), save it on the elastisearch
    for item in stream.get_iterator():
        try:
            # Saving the tweet on the ES
            es.index(
                index="tweets",
                doc_type="tweet",
                body=patch_tweet(item)
            )
        except:
           #caso haja qualquer problema com o elasticsearch ele verifica o estado e reinicia se necessário
            check_es_status()
            es = elasticsearch.Elasticsearch(["<dominio_do_servidor>:9200"])
            print ("Getting back to tweet recording")