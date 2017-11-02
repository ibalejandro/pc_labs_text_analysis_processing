# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep


class WordInDocs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        word, doc_names_as_string = line.split(";;;")
        doc_names = doc_names_as_string.split(";;")
        yield word, doc_names

    def reducer(self, word, doc_names):
        for doc_name in doc_names:
            r.lpush("word:" + word, *doc_name)
            print(word, doc_name)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    WordInDocs.run()