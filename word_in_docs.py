from mrjob.job import MRJob
import redis

class WordInDocs(MRJob):

    def mapper(self, _, line):
        doc_with_similarity = []
        word, value = line.decode('ISO-8859-1', 'ignore').split(';;;')
        docs = value.decode('ISO-8859-1', 'ignore').split(';;')
        yield word, docs

    def reducer(self, key, values):
        for value in values:
            r.lpush("word:" + key, *value)
            yield key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost', port=6379, db=7)
    WordInDocs.run()