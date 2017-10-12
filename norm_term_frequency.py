from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os

WORD_RE = re.compile(r"[\w']+")


# Calculates the normalized Term Frequency (TF) using the given documents.
class MRNormTermFrequency(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_doc_name_and_word,
                   combiner=self.combiner_sum_occurrences_for_doc_name_and_word,
                   reducer=self.reducer_calculate_word_and_norm_cumulative_occurrences_for_doc_name)
        ]

    # Yields [(document name, word), occurrence] for each word in the line.
    def mapper_get_occurrence_for_doc_name_and_word(self, _, line):
        # Gets the input file name.
        try:
            doc_name = os.getenv('mapreduce_map_input_file')
        except KeyError:
            doc_name = os.getenv('map_input_file')

        # In order to yield a pair, the word has to match the RegularExpression WORD_RE.
        for word in WORD_RE.findall(line.decode('utf-8', 'ignore')):
            yield (doc_name, word.lower()), 1

    # Yields [document name, (word, cumulative_occurrences)] for each (document_name, word) key received.
    def combiner_sum_occurrences_for_doc_name_and_word(self, doc_name_and_word, occurrences):
        doc_name, word = doc_name_and_word
        yield doc_name, (word, sum(occurrences))

    # Prints [document name, word, norm_cumulative_occurrences] for each document.
    def reducer_calculate_word_and_norm_cumulative_occurrences_for_doc_name(self, doc_name,
                                                                            word_and_cumulative_occurrences):
        # Converts the word_and_cumulative_occurrences (Generator) to a list of tuples.
        word_and_cumulative_occurrences_list = []
        # Counts the number of terms in the document.
        number_of_terms = 0
        for word, cumulative_occurrences in word_and_cumulative_occurrences:
            word_and_cumulative_occurrences_list.append((word, cumulative_occurrences))
            number_of_terms += cumulative_occurrences

        # The number of terms will be the denominator for the normalization and it must be cast to float in order to
        # obtain a floating point division.
        number_of_terms_as_float = float(number_of_terms)
        for i in range(0, len(word_and_cumulative_occurrences_list)):
            word, cumulative_occurrences = word_and_cumulative_occurrences_list[i]
            # The normalized cumulative occurrences are the cumulative occurrences divided by the number of terms in
            # the document.
            norm_cumulative_occurrences = cumulative_occurrences / number_of_terms_as_float
            # The cumulative occurrences for each word are replaced by the normalized cumulative occurrences.
            word_and_cumulative_occurrences_list[i] = (word, norm_cumulative_occurrences)

        # Formats the output to write is as CSV.
        for word, norm_cumulative_occurrences in word_and_cumulative_occurrences_list:
            row = doc_name + "," + word + "," + ('%.6f' % norm_cumulative_occurrences)
            print row


if __name__ == '__main__':
    MRNormTermFrequency.run()
