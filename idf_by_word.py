# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep


class IDFByWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        word, idf = line.split(";;")
        yield word, idf

    def reducer(self, word, idfs):
        for idf in idfs:
            r.set("idf:" + word, idf)
            print(word, idf)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    IDFByWord.run()
