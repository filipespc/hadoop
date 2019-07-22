-- Pig script to print all the movies with average rating less then 2.0 stars,
-- ordered by its popularity (number of reviews).
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

ratingsByMovies = GROUP ratings BY movieID;

moviesRatingsInfo = FOREACH ratingsByMovies GENERATE group AS movieID, AVG(ratings.rating) as avgRating, COUNT(ratings.userID) as popularity;

badMovies = FILTER moviesRatingsInfo BY avgRating < 2.0;

badMoviesNamed = JOIN badMovies BY movieID, nameLookup BY movieID;

mostPopularBadMovies = ORDER badMoviesNamed BY badMovies::popularity DESC;

final = FOREACH mostPopularBadMovies GENERATE nameLookup::movieTitle AS movieTitle, 
	badMovies::popularity AS popularity, badMovies::avgRating AS avgRating;

DUMP final;