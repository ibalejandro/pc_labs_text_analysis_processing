from mrjob.job import MRJob
import redis

# Stores in Redis, the similarity documents for a document.
class MRSimilarityDoc(MRJob):

    # Yields the similarity document with another, and viceversa.
    def mapper(self, _, line):
        # Splits row string by ';;' characters.
        doc_name_1, doc_name_2, similarity = line.split(';;')
        # Yields the similarity of the document 2 with the document 1
        yield doc_name_1, (doc_name_2, similarity)
        # And viceversa.
        yield doc_name_2, (doc_name_1, similarity)

    # Yields [document name 1, (document name 2, similarity)]
    def reducer(self, key, values):
        # Iterates each record related to one document
        for value in values:
            # Stores a hash map with the similarity between two documents
            r.hset("similarity:" + key, value[0], value[1])
            print key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='gutenberg-ir.redis.cache.windows.net', port=6380, db=1,
                          password='B4qWA879R/U2ldA3mWT5kcJSHrDXOijbd9ju+89PNhg=', ssl=True)
    MRSimilarityDoc.run()