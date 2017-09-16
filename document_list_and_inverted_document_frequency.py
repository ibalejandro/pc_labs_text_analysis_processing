from mrjob.job import MRJob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol
import re
import os
import math

WORD_RE = re.compile(r"[\w']+")
TOTAL_NUMB_OF_DOCUMENTS = 3  # This value needs to be updated with the real total number of documents.


# For each word, calculates which documents contain it and its Inverted Document Frequency using the given documents.
class MRDocumentListAndInvertedDocumentFrequency(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # Writes output as CSV.

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_word_and_doc_name,
                   combiner=self.combiner_sum_occurrences_for_word_and_doc_name,
                   reducer=self.reducer_sort_doc_names_and_get_idf_for_word),
            MRStep(reducer=self.reducer_sort_doc_names_and_idf_for_word)
        ]

    # Yields [(word, document name), occurrence] for each word in the line.
    def mapper_get_occurrence_for_word_and_doc_name(self, _, line):
        # Gets the input file name.
        try:
            doc_name = os.getenv('mapreduce_map_input_file')
        except KeyError:
            doc_name = os.getenv('map_input_file')

        # In order to yield a pair, the word has to match the RegularExpression WORD_RE.
        for word in WORD_RE.findall(line.decode('utf-8', 'ignore')):
            yield (word.lower(), doc_name), 1

    # Yields [word, (document name, cumulative_occurrences)] for each (word, document_name) key received.
    def combiner_sum_occurrences_for_word_and_doc_name(self, word_and_doc_name, occurrences):
        word, doc_name = word_and_doc_name
        yield word, (doc_name, sum(occurrences))

    '''
    Yields {None, [word, (document name list, inverted document frequency)]} for each word after sorting the document
    name list in descendant order using the cumulative occurrences of the word as criterion.
    '''

    def reducer_sort_doc_names_and_get_idf_for_word(self, word, doc_name_and_cumulative_occurrences):
        # Converts the doc_name_and_cumulative_occurrences (Generator) to a list of tuples.
        doc_name_and_cumulative_occurrences_list = []
        for doc_name, cumulative_occurrences in doc_name_and_cumulative_occurrences:
            doc_name_and_cumulative_occurrences_list.append((doc_name, cumulative_occurrences))

        # Sorts the tuple list in descendant order using the cumulative occurrences as criterion.
        doc_name_and_cumulative_occurrences_list.sort(key=lambda x: x[1], reverse=True)

        # Creates a list containing only the document names after the descendant sorting.
        doc_name_list = []
        for i in range(0, len(doc_name_and_cumulative_occurrences_list)):
            # The document name is at the first position of the tuple.
            doc_name_list.append(doc_name_and_cumulative_occurrences_list[i][0])

        # The length of the doc_name_list means the number of documents in which the word appears.
        numb_of_documents = len(doc_name_list)
        # Calculates the Inverted Document Frequency using its formula. The denominator must be cast to float in order
        # to obtain a floating point division.
        inverted_document_frequency = 1 + math.log(TOTAL_NUMB_OF_DOCUMENTS / float(numb_of_documents))
        yield None, (word, (doc_name_list, inverted_document_frequency))

    '''
    Receives every pair produced by the previous Reducer because the yielded key was None.
    Yields [word, (document name list, inverted document frequency)] for each word after sorting the words in
    ascendant order using the Inverted Document Frequency as criterion (i.e. the words are sorted in descendant order
    according to the number of documents in which they appear).
    '''

    def reducer_sort_doc_names_and_idf_for_word(self, _, word_doc_name_list_and_idf):
        # Converts the word_doc_name_list_and_idf (Generator) to a list of tuples.
        word_doc_name_list_and_idf_list = []
        for word, (doc_name_list, idf) in word_doc_name_list_and_idf:
            word_doc_name_list_and_idf_list.append((word, (doc_name_list, idf)))

        # Sorts the tuple list in ascendant order using the Inverted Document Frequency as criterion.
        word_doc_name_list_and_idf_list.sort(key=lambda x: x[1][1])

        # Yields every record in the correct order and formats the output to write is as CSV.
        for word, (doc_name_list, idf) in word_doc_name_list_and_idf_list:
            for doc_name in doc_name_list:
                yield (None, (word, doc_name, idf))


if __name__ == '__main__':
    MRDocumentListAndInvertedDocumentFrequency.run()
