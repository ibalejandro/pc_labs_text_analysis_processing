from mrjob.job import MRJob
import redis

class IDFbyWord(MRJob):

    def mapper(self, _, line):
        doc_with_similarity = []
        word, value = line.decode('ISO-8859-1', 'ignore').split(';;')
        yield word, value

    def reducer(self, key, values):
        for value in values:
            r.set("idf:" + key, value)
            yield key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost', port=6379, db=7)
    IDFbyWord.run()