Driver: RecommenderDriver is the driver for jobone and jobtwo
jobone:
	Mapper:
		This job is the first job that takes the input data set and produces the following result to the reducer
		output: customerID, movieID, rating ⇒ key: customerID, value: (movieID, rating)
	Reducer:
		The reducer takes the input from the mapper and produces the a list of movies and its rate value provided
		by the current user
		output: customerID, (movieID, rating) ⇒ key: customerID, value would be a list of (movieID, rating): (movieID_1, rating_1) (movieID_2, rating_2) · · · (movieID_n, rating_n)

jobtwo:
	This job is the second job, the input from job1 and produces the similarity between two movies
	Mapper:
		The job produces the following output 
		customerID, (movieID_1, rating_1) (movieID_2, rating_2) ··· (movieID_n, rating_n) ⇒ key: a pair of movies: (movieID_i, movie_j), value: a pair of corresponding ratings: (rating_i, rating_j)
	Reducer:
		Two similarity functions have been tried to compute the similarity.
		1. Pearsons Correlation
		2. Cosine Similarity	

jobthree:
	Mapper: 
		An argument, which is the CustomerID, is passed to the mapper which retrieves the records where the particular
		customer is involved.
	Driver:
		Run the customer job by providing the CID as a command line argument
		
jobfour:
	Mapper: 
		An argument, which is the MovieID, is passed to the mapper which retrieves the records where the particular
		movie is involved.
	Driver:
		Run the customer job by providing the MID as a command line argument
		
		
		