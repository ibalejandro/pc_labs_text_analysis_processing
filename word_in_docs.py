# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep

# Class that stores docs results list for every word in Redis,
class WordInDocs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Yields document list for each word in the line.
    def mapper(self, _, line):
        # Split row string by ';;;' characters.
        word, doc_names_as_string = line.split(";;;")
        # Split the documents string by ';;' characters.
        doc_names = doc_names_as_string.split(";;")
        # Yield every word with its documents list.
        yield word, doc_names

    # Yields [document list] for each word in the line.
    def reducer(self, word, doc_names):
        for doc_name in doc_names:
            # Store document list with word as a key in Redis.
            r.lpush("word:" + word, *doc_name)
            # Print word with its document list.
            print(word, doc_name)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    WordInDocs.run()