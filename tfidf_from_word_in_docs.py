# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep

# Stores the TfIdf for each word in Redis.
class MRTfidfFromWordInDocs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Yields tfidf for tuple doc name with word.
    def mapper(self, _, line):
        # Obtains doc name, word and tfidf for line.
        doc_name, word, tfidf = line.split(";;")
        # Composes a key with the tuple doc name and word.
        key = doc_name + ";;" + word
        yield key, tfidf

    # Yield [(document name, word), tfidf].
    def reducer(self, key, tfidfs):
        for tfidf in tfidfs:
            # Stores the tfidf for tuple document name and word.
            r.set("tfidf:" + key, tfidf)
            print(key, tfidf)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    MRTfidfFromWordInDocs.run()
