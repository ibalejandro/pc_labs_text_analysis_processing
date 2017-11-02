# coding=utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep
import redis

class MagnitudeByDoc2(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Yields [(document name, word), occurrence] for each word in the line.
    def mapper(self, _, line):
        # In order to yield a pair, the word has to pass the validation filter.
        doc_name, magnitude = line.split(";;")
        yield doc_name, magnitude

    # Yields [document name, (word, cumulative_occurrences)] for each (document_name, word) key received.
    def reducer(self, doc_name, magnitudes):
        for magnitude in magnitudes:
            r.set("magnitude:" + doc_name, magnitude)
            print(doc_name, magnitude)


if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    MagnitudeByDoc2.run()
