# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        parts = line.split(',')
        
        if len(parts) == 4 and parts[0] != 'userId':
            userId, movieId, tag, timestamp = parts
            
            yield userId, 1

    def reducer_count_tags(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TagsPerMovie.run()
