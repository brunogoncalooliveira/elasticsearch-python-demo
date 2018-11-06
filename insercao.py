from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from timeit import default_timer as timer

es = Elasticsearch("http://127.0.0.1:9200/")
es.indices.create(index='nomes', ignore=400) #ignore=400 --> ignora se já existir

start = timer()


fi = open('data.txt')
txt = fi.read()
fi.close()

pessoa = []
first = True

for reg in txt.split("\n"):
    registo = reg.split('#')
    nome = registo[0].strip()
    if nome != '':
        pessoa.append({'nome': nome, 'abertura': registo[1], 'balcao': registo[2]})


threads = []

start = timer()
cnt = 0
data_list = []

def gendata(t_pessoa):
    for reg in t_pessoa:
        yield {
            "_index": "nomes",
            "_type": "document",
            "conta": reg,
        }

def worker(data_list):
    bulk(es, gendata(data_list))

print('inicio')
for i in pessoa:
    data_list.append(i)
    if cnt % 1000 == 0:
        print(cnt)
        bulk(es, gendata(data_list))
        data_list = []

    cnt += 1

es.indices.refresh()
end = timer()

end = timer()
print('tempo: ' + str(end - start)) # Time in seconds, e.g. 5.38091952400282

print(end - start) # Time in seconds, e.g. 5.38091952400282
