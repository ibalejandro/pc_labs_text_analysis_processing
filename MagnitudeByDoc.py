from mrjob.job import MRJob
import redis

class MagnitudeByDoc(MRJob):

    def mapper(self, _, line):
        key, magnitude = line.decode('ISO-8859-1', 'ignore').split(';;');
        yield key, magnitude

    def reducer(self, key, values):
        for value in values:
            r.set("magnitude:"+key, value)
            yield key, value

if __name__ == '__main__':
    r = redis.StrictRedis(host='localhost', port=6379, db=7)
    MagnitudeByDoc.run()