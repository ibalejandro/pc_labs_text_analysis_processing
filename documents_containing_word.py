from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os

WORD_RE = re.compile(r"[\w']+")


# Calculates which documents contain each word for the given documents.
class MRDocumentsContainingWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_word_and_doc_name,
                   combiner=self.combiner_sum_occurrences_for_word_and_doc_name,
                   reducer=self.reducer_sort_inverted_file_index_and_get_doc_names_for_word)
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
    Yields [word, list(document name)] for each word after sorting the list in descendant order using the cumulative
    occurrences as criterion.
    '''

    def reducer_sort_inverted_file_index_and_get_doc_names_for_word(self, word, doc_name_and_cumulative_occurrences):
        # Converts the doc_name_and_cumulative_occurrences (Generator) to a list of tuples.
        doc_name_and_cumulative_occurrences_list = []
        for doc_name, cumulative_occurrences in doc_name_and_cumulative_occurrences:
            doc_name_and_cumulative_occurrences_list.append((doc_name, cumulative_occurrences))

        # Sorts the tuple list in descendant order using the cumulative occurrences as criterion.
        doc_name_and_cumulative_occurrences_list.sort(key=lambda x: x[1], reverse=True)

        # Creates a list containing only the document names after the descendant sorting.
        doc_name_list = []
        for i in range(0, len(doc_name_and_cumulative_occurrences_list)):
            # The document name is the first position of the tuple.
            doc_name_list.append(doc_name_and_cumulative_occurrences_list[i][0])
        yield word, doc_name_list


if __name__ == '__main__':
    MRDocumentsContainingWord.run()
