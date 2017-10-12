import math
import os
import re

from mrjob.job import MRJob
from mrjob.step import MRStep

WORD_RE = re.compile(r"[\w']+")
TOTAL_NUMB_OF_DOCUMENTS = 3  # This value needs to be updated with the real total number of documents.


# For each word, calculates its Inverted Document Frequency using the given documents.
class MRInvertedDocumentFrequency(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_word_and_doc_name,
                   combiner=self.combiner_get_unique_occurrence_for_word_in_doc_name,
                   reducer=self.reducer_calculate_idf_for_word)
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
            yield (word.lower(), doc_name), None

    # Yields [word, document name] for each (word, document_name) key received.
    def combiner_get_unique_occurrence_for_word_in_doc_name(self, word_and_doc_name, _):
        word, doc_name = word_and_doc_name
        yield word, doc_name

    # Prints [word, inverted document frequency] for each word.
    def reducer_calculate_idf_for_word(self, word, doc_names):
        # Iterates over the doc_names (Generator) to count the number of documents in which the word appears.
        numb_of_documents = 0
        for doc_name in doc_names:
            numb_of_documents += 1

        # Calculates the Inverted Document Frequency using its formula. The denominator must be cast to float in order
        # to obtain a floating point division.
        inverted_document_frequency = math.log10(TOTAL_NUMB_OF_DOCUMENTS / float(numb_of_documents))

        # Formats the output to write is as CSV.
        row = word + "," + ('%.6f' % inverted_document_frequency)
        print row


if __name__ == '__main__':
    MRInvertedDocumentFrequency.run()
