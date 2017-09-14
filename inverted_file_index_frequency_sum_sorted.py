from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os

WORD_RE = re.compile(r"[\w']+")


# Calculates the Inverted File Index sorted by frequency sum for the given documents.
class MRInvertedFileIndexFrequencySumSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_word_and_doc_name,
                   combiner=self.combiner_sum_occurrences_for_word_and_doc_name,
                   reducer=self.reducer_sort_inverted_file_index_for_word),
            MRStep(reducer=self.reducer_sort_inverted_file_index_by_frequency_sum)
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
    Yields {None, {[word, list(document name, cumulative_occurrences)], criterion_for_next_reduction}} for each word
    after sorting the list in descendant order using the cumulative occurrences as local criterion.
    '''

    def reducer_sort_inverted_file_index_for_word(self, word, doc_name_and_cumulative_occurrences):
        # Converts the doc_name_and_cumulative_occurrences (Generator) to a list of tuples.
        doc_name_and_cumulative_occurrences_list = []
        word_frequency_sum = 0
        for doc_name, cumulative_occurrences in doc_name_and_cumulative_occurrences:
            doc_name_and_cumulative_occurrences_list.append((doc_name, cumulative_occurrences))
            word_frequency_sum += cumulative_occurrences

        # Sorts the tuple list in descendant order using the cumulative occurrences as local criterion.
        doc_name_and_cumulative_occurrences_list.sort(key=lambda x: x[1], reverse=True)

        # Uses the Inverted File Index frequency sum for each word as the criterion for the next reduction. That will
        # mean to sort the entire Inverted File Index in descendant order by the times the word appears taking the whole
        # documents into account.
        criterion_for_next_reduction = word_frequency_sum
        yield None, ((word, doc_name_and_cumulative_occurrences_list), criterion_for_next_reduction)

    '''
    Receives every pair produced by the previous Reducer because the yielded key was None.
    Yields [word, list(document name, cumulative_occurrences)] for each word after sorting the complete Inverted File
    Index in descendant order using the times the word appears taking the whole documents into account as criterion.
    '''

    def reducer_sort_inverted_file_index_by_frequency_sum(self, _, word_ifi_and_criterion):
        # Converts the word_ifi_and_criterion (Generator) to a list of tuples.
        word_ifi_and_criterion_list = []
        for (word, ifi), criterion in word_ifi_and_criterion:
            word_ifi_and_criterion_list.append(((word, ifi), criterion))

        # Sorts the tuple list in descendant order using the times the word appears taking the whole documents into
        # account as criterion.
        word_ifi_and_criterion_list.sort(key=lambda x: x[1], reverse=True)

        # Yields every record of the Inverted File Index in the correct order.
        for (word, ifi), criterion in word_ifi_and_criterion_list:
            yield (word, ifi)[0], (word, ifi)[1]

if __name__ == '__main__':
    MRInvertedFileIndexFrequencySumSorted.run()
