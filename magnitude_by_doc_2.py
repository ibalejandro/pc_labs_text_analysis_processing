# coding=utf-8

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRNormTermFrequency(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_doc_name_and_word,
                   reducer=self.reducer_sum_occurrences_for_doc_name_and_word)
        ]

    # Yields [(document name, word), occurrence] for each word in the line.
    def mapper_get_occurrence_for_doc_name_and_word(self, _, line):
        # In order to yield a pair, the word has to pass the validation filter.
        print(line)
        doc_name, magnitude = line.split(";;")
        yield doc_name, magnitude

    # Yields [document name, (word, cumulative_occurrences)] for each (document_name, word) key received.
    def reducer_sum_occurrences_for_doc_name_and_word(self, doc_name, magnitudes):
        for magnitude in magnitudes:
            yield doc_name, magnitude

if __name__ == '__main__':
    MRNormTermFrequency.run()
