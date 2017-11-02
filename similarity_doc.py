from mrjob.job import MRJob
import redis

class SimilarityDoc(MRJob):

    def mapper(self, _, line):
        doc_name_1, doc_name_2, similarity = line.decode('ISO-8859-1', 'ignore').split(';;')
        yield doc_name_1, (doc_name_2, similarity)
        yield doc_name_2, (doc_name_1, similarity)

    def reducer(self, key, values):
        for value in values:
            r.hset("similarity:" + key, value[0], value[1])
            yield key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    SimilarityDoc.run()