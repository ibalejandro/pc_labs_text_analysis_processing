from mrjob.job import MRJob
import redis

class TfidfFromWordInDocs(MRJob):

    def mapper(self, _, line):
        docname, word, tfidf = line.decode('ISO-8859-1', 'ignore').split(';;');
        key = docname+';;'+word
        print key, tfidf
        yield key, tfidf

    def reducer(self, key, values):
        for value in values:
            r.set("tfidf:"+key, value)
            yield key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost', port=6379, db=7)
    TfidfFromWordInDocs.run()