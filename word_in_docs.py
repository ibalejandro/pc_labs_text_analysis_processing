# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep

# Class that stores docs results list for every word in Redis.
class MRWordInDocs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Yields document list for each word in the line.
    def mapper(self, _, line):
        # Splits row string by ';;;' characters.
        word, doc_names_as_string = line.split(";;;")
        # Split the documents string by ';;' characters.
        doc_names = doc_names_as_string.split(";;")
        for doc_name in doc_names:
            # Yield every word with its documents list.
            yield word, doc_name

    # Yields [document list] for each word in the line.
    def reducer(self, word, doc_names):
        for doc_name in doc_names:
            # Splits doc name with the second parameter can specifies the maximum number of splits to perform.
            # It is necessary to remove the first part of string "hdfs:///sandbox.hortonworks.com:"
            doc_name = doc_name.split(':', 2)[2]
            # Stores document list with word as key in Redis.
            r.lpush("word:" + word, doc_name)
            # Prints word with its document list.
            print(word, doc_name)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    MRWordInDocs.run()