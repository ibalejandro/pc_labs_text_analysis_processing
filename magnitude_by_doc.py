# coding=utf-8
import redis
from mrjob.job import MRJob
from mrjob.step import MRStep


class MagnitudeByDoc(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        key, magnitude = line.split(";;")
        yield key, magnitude

    def reducer(self, key, values):
        for value in values:
            r.set("magnitude:" + key, value)
            print(key, value)


if __name__ == '__main__':
    r = redis.from_url(
        "redis://h:padd1089bb3eef4b1bf8c5cd5019461d8f7ad76b4c6960640f882ce0f2a9c86a6@ec2-34-224-49-43.compute-1.amazonaws.com:65139",
        db=1)
    MagnitudeByDoc.run()
