from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import math

WORD_RE = re.compile(r"[\w']+")
TOTAL_NUMB_OF_DOCUMENTS = 2  # This value needs to be updated with the real total number of documents.


# Calculates the Inverted Document Frequency for the given documents.
class MRInvertedDocumentFrequency(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_word_and_doc_name,
                   combiner=self.combiner_sum_occurrences_for_word_and_doc_name,
                   reducer=self.reducer_get_inverted_document_frequency_for_word),
            MRStep(reducer=self.reducer_sort_inverted_document_frequency)
        ]

    # Yields [(word, document name), occurrence] for each word in the line.
    def mapper_get_occurrence_for_word_and_doc_name(self, _, line):
        # Gets the input file name.
        try:
            doc_name = os.getenv('mapreduce_map_input_file')
        except KeyError:
            doc_name = os.getenv('map_input_file')

        # In order to yield a pair, the word has to match the RegularExpression WORD_RE.
        for word in WORD_RE.findall(line):
            yield (word.lower(), doc_name), 1

    # Yields [word, (document name, cumulative_occurrences)] for each (word, document_name) key received.
    def combiner_sum_occurrences_for_word_and_doc_name(self, word_and_doc_name, occurrences):
        word, doc_name = word_and_doc_name
        yield word, (doc_name, sum(occurrences))

    '''
    Yields [None, (word, inverted document frequency)] for each word using the total number of documents and the number
    of documents in which the word appears.
    '''

    def reducer_get_inverted_document_frequency_for_word(self, word, doc_name_and_cumulative_occurrences):
        # Converts the doc_name_and_cumulative_occurrences (Generator) to a list of tuples.
        doc_name_and_cumulative_occurrences_list = []
        for doc_name, cumulative_occurrences in doc_name_and_cumulative_occurrences:
            doc_name_and_cumulative_occurrences_list.append((doc_name, cumulative_occurrences))
        # The length of the doc_name_and_cumulative_occurrences_list means the number of documents in which the word
        # appears.
        numb_of_documents = len(doc_name_and_cumulative_occurrences_list)
        # Calculates the Inverted Document Frequency using its formula.
        inverted_document_frequency = 1 + math.log(TOTAL_NUMB_OF_DOCUMENTS / numb_of_documents)
        yield None, (word, inverted_document_frequency)

    '''
    Receives every pair produced by the previous Reducer because the yielded key was None.
    Yields (word, inverted document frequency) for each word after sorting the complete Inverted Document Frequency in
    descendant order using the Inverted Document Frequency as criterion.
    '''

    def reducer_sort_inverted_document_frequency(self, _, word_and_inverted_document_frequency):
        # Converts the word_and_inverted_document_frequency (Generator) to a list of tuples.
        word_and_inverted_document_frequency_list = []
        for word, inverted_document_frequency in word_and_inverted_document_frequency:
            word_and_inverted_document_frequency_list.append((word, inverted_document_frequency))

        # Sorts the tuple list in descendant order using the Inverted Document Frequency as criterion.
        word_and_inverted_document_frequency_list.sort(key=lambda x: x[1], reverse=True)

        # Yields every record of the Inverted Document Frequency in the correct order.
        for word, inverted_document_frequency in word_and_inverted_document_frequency_list:
            yield word, inverted_document_frequency


if __name__ == '__main__':
    MRInvertedDocumentFrequency.run()
