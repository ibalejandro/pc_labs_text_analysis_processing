from mrjob.job import MRJob
import redis

class SimilarityDoc(MRJob):

    # def mapper(self, _, line):
    #     doc_with_similarity = []
    #     key, value = line.decode('ISO-8859-1', 'ignore').split(';;;');
    #     values = value.decode('ISO-8859-1', 'ignore').split(';;');
    #     for i in xrange(0, len(values), 2):
    #         doc_name = values[i]
    #         similarity = values[i+1]
    #         doc_with_similarity.append([doc_name, similarity])
    #     yield key, doc_with_similarity

    def mapper(self, _, line):
        doc_with_similarity = []
        key, doc_name, similarity = line.decode('ISO-8859-1', 'ignore').split(';;')
        doc_with_similarity.append([doc_name, similarity])
        yield key, doc_with_similarity

    def reducer(self, key, values):
        for value in values:
            for item in value:
                print ("item", item)
                r.hset("similarity:"+key, item[0], item[1])
            print(len(value))
            yield key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost', port=6379, db=7)
    SimilarityDoc.run()