# coding=utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep


class MagnitudeByDoc2(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # Yields [(document name, word), occurrence] for each word in the line.
    def mapper(self, _, line):
        # In order to yield a pair, the word has to pass the validation filter.
        yield line, 1

    # Yields [document name, (word, cumulative_occurrences)] for each (document_name, word) key received.
    def reducer(self, line, counts):
        for count in counts:
            print(line, count)

if __name__ == '__main__':
    MagnitudeByDoc2.run()
