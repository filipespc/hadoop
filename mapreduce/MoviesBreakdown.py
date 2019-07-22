'''
    This file is an exercise of the course "The Ultimate Hands-On Hadoop - Tame your Big Data!"
    It was designed to compute how many times each movie has been rated.
'''
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies)
        ]

    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movies(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MoviesBreakdown.run()