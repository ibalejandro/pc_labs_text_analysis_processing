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
            print(doc_name, magnitude)


if __name__ == '__main__':
    r = redis.from_url("redis://h:padd1089bb3eef4b1bf8c5cd5019461d8f7ad76b4c6960640f882ce0f2a9c86a6@ec2-34-224-49-43.compute-1.amazonaws.com:65139", db=1)
    MagnitudeByDoc2.run()
