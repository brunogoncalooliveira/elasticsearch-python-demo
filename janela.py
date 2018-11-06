# encoding: utf8
from __future__ import unicode_literals
import tkinter as tk
import pygubu
from elasticsearch import Elasticsearch

from timeit import default_timer as timer

es = Elasticsearch("http://127.0.0.1:9200/")

class Application:
    def __init__(self, master):
        self.master = master

        self.builder = builder = pygubu.Builder()
        builder.add_from_file('janela.ui')
        self.mainwindow = builder.get_object('mainwindow', master)
        self.texto = self.builder.get_object('texto')
        self.tempo = self.builder.get_variable('lbltempo')
        self.searchtype = self.builder.get_variable('searchtype')
        self.searchtype.set('Search as you type')

        builder.connect_callbacks(self)


    def on_name_change(self, nome):
        """
        query = {
            "match_phrase_prefix" : {
                "conta.nome" : nome
            }
        }

        {
          "query" : {
            "bool" : {
              "must": [{
                "match": {
                  "conta.nome" : "joao"
                }
              }, {
                "match": {
                  "conta.nome" : "oliveira"
                }
              }]
            }
          }
        }
        """
        
        if self.searchtype.get() == 'Search as you type':
            query = {
                "match_phrase_prefix" : {
                    "conta.nome" : nome
                }
            }
        else:
            must = []
            for i in nome.split(' '):
                reg = {
                    "match": {
                      "conta.nome" : i
                    }
                  }
                if i != '':
                    must.append(reg)
            query = {
                "bool" : {
                  "must": must
                }
              }


        start = timer()
        res = es.search(index="nomes", body={"query": query, "size": 10})
        end = timer()
        t_total = '{:3.3f}s'.format(end - start)
        #pprint(res)

        txt = 'NOME                                     |   ABERTURA   | BALCAO' + "\n"
        txt += '-----------------------------------------+--------------+-------'+ "\n"
        for t in res['hits']['hits']:
            reg = t['_source']['conta']
            txt += reg['nome'].ljust(40) + ' |  ' + reg['abertura'].ljust(11) + ' | ' + reg['balcao'].rjust(6) + "\n"
        txt = txt.strip()

        self.texto.delete("1.0", "15.0")
        self.texto.insert("1.0", txt)

        matched = res['hits']['total']
        self.tempo.set('matched: ' + str(matched) + "\n" + 'tempo -> elasticsearch (' + str(res['took']) + 'ms) total (' + t_total +')')

        return True


if __name__ == '__main__':
    root = tk.Tk()
    app = Application(root)
    root.mainloop()