-- Pig script to print all the movies with average rating greater then 4.0 stars,
-- ordered by its production date.
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate,'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

moviesRated = FOREACH ratingsByMovie GENERATE group AS movieID,
	AVG(ratings.rating) AS avgRating;

fiveStarsMovies = FILTER moviesRated BY avgRating > 4.0;

fiveStarsNamed = JOIN fiveStarsMovies BY movieID, nameLookup BY movieID;

oldiesButGoodies = ORDER fiveStarsNamed BY nameLookup::releaseTime;

DUMP oldiesButGoodies;