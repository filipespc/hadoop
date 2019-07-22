'''
    This file is an exercise of the course "The Ultimate Hands-On Hadoop - Tame your Big Data!"
    It was designed to compute how many times each movie has been rated.
'''
from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviesBreakdownSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies),
            MRStep(reducer=self.reducer_sort_by_count)
        ]

    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movies(self, movie_id, one):
        yield str(sum(one)).zfill(5), movie_id

    def reducer_sort_by_count(self, count, movie_ids):
        for movie_id in movie_ids:
            yield movie_id, count

if __name__ == '__main__':
    MoviesBreakdownSorted.run()