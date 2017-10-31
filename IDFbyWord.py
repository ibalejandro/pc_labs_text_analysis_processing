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
    r = redis.from_url(
        "redis://h:padd1089bb3eef4b1bf8c5cd5019461d8f7ad76b4c6960640f882ce0f2a9c86a6@ec2-34-224-49-43.compute-1.amazonaws.com:65139",
        db=1)
    IDFbyWord.run()