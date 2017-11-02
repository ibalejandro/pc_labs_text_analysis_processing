# coding=utf-8

import redis
from mrjob.job import MRJob
from mrjob.step import MRStep


class WordInDocs(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        word, doc_names_as_string = line.split(";;;")
        doc_names = doc_names_as_string.split(";;")
        yield word, doc_names

    def reducer(self, word, doc_names):
        for doc_name in doc_names:
            r.lpush("word:" + word, *doc_name)
            print(word, doc_name)


if __name__ == '__main__':
    r = redis.from_url(
        "redis://h:padd1089bb3eef4b1bf8c5cd5019461d8f7ad76b4c6960640f882ce0f2a9c86a6@ec2-34-224-49-43.compute-1.amazonaws.com:65139",
        db=1)
    WordInDocs.run()
