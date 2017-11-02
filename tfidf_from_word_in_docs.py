# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep


class TfidfFromWordInDocs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        doc_name, word, tfidf = line.split(";;");
        key = doc_name + ";;" + word
        yield key, tfidf

    def reducer(self, key, tfidfs):
        for tfidf in tfidfs:
            r.set("tfidf:" + key, tfidf)
            print(key, tfidf)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    TfidfFromWordInDocs.run()
