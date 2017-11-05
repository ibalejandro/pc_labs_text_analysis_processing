# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep

# Class that takes IDF of each word for then store in Redis.
class MRIDFByWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Yields idf for each word in the line.
    def mapper(self, _, line):
        # Splits row string by ';;' characters.
        word, idf = line.split(";;")
        # Yield word with its idf calculated.
        yield word, idf

    # Yields idf for each word in the line.
    def reducer(self, word, idfs):
        for idf in idfs:
            # Stores idf with word as key in Redis.
            r.set("idf:" + word, idf)
            # Prints word with its idf.
            print(word, idf)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    MRIDFByWord.run()
