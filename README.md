# teste-case-twitter
Desafio de estudo de caso utilizando API Twitter com os seguintes critérios:

Case Twitter

1.	Crie uma aplicação na linguagem que desejar para coletar as últimas postagens do Twitter, dada uma determinada #tag.
a.	Por padrão o Twitter disponibiliza as 100 últimas postagens.
b.	Caso não tenha 100 twittes, colete todas que vierem.
c.	Não há necessidade de coletar mais do que 100 twittes, dada um #tag.
d.	Atente-se a limites e timeouts

2.	Use uma base de dados para armazenar as informações.

3.	Colete e armazene as mensagens, na base de dados, para as #tags listadas abaixo:
 (#openbanking, #remediation, #devops, #sre, #microservices, #observability, #oauth, #metrics, #logmonitoring, #opentracing)

4.	Utilizando uma linguagem de sua preferência, sumarize e grave os dados para conseguir listar as informações:
a.	Quais são os 5 (cinco) usuários, da amostra coletada, que possuem mais seguidores?
b.	Qual o total de postagens, agrupadas por hora do dia (independentemente da #hashtag)? 
c.	Qual o total de postagens para cada uma das #tag por idioma/país do usuário que postou;

5.	Crie uma API REST que permita o consumo dos três itens anteriores. A API deverá expor métricas de execução. 

6.	Crie uma coleção no postman para consumir as APIs criadas.

7.	Envie seus logs para uma ferramenta de logging (exemplos: Elastic Search, Splunk, Graylog ou similar) e crie uma query que mostre em tempo real os eventos que acontecem na execução da API criada acima, exemplos (Warning, Erro, Debug, Info, etc).

8.	Utilize uma ferramenta de métricas para sua infraestrutura (exemplos: Prometheus, Zabbix, cloudwatch ou similar) e também crie 3 dashboards que mostrem em tempo real a quantidade de execução, a latência (tempo de execução) e quantidade de erros da API criada acima.

 
9.	Publique o projeto no Github e documente no README.md os itens abaixo:
a.	Documentação do projeto
b.	Documentação das APIs 
c.	Documentação de arquitetura
d.	Documentação de como podemos subir uma cópia deste ambiente
e.	Manual com prints dos Logs (item 7) e os Dashboards (item 9) explicando cada um deles.

10. Bônus. Se você chegou até aqui e quiser, pode criar um frontend para sua aplicação para facilitar a experiencia do usuário.

#Visão do projetos e objetivos
A visão principal do projeto é:

 • Logs (Filebeat)
 • Metricas 
 • Traces
 • Uptime
 • Segurança

O contexto se encaixa como um atributo que o seu sistema oferece em HA, estabilidade, Usuabilidade.

A observabilidade


