# Amazon Reviews 2

This MapReduce application analyzes Amazon customer reviews, which a set of review data has been made publicly available by Amazon. Information about this data set can be found here: https://s3.amazonaws.com/amazon-reviews-pds/readme.html. Details about the structure of the data and different files in the data set can be found here: https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt

This is primarily interested in the “star_rating” (the rating of a product) and “customer_id” (the id of a customer) columns of the data set. The file format is Tab Separated Values (TSV), which uses tab characters to separate the fields in each row. Each line of the file represents one data record, except that the first row contains headers (i.e. the names of the fields).

To compute pairs of users who gave the rating of at least 4 to at least three common products, this chained job requires two Mappers, two Reducers, and a Custom WritableComparable (i.e. UserPairWritable). The first job creates the pairs of users who gave a rating of at least 4 to a common product. The second job aggregates all products of the same pair of users and outputs the pair of users if there are at least three common products. The output should contain three columns (customer_id, customer_id, list of common products). The output is sorted by the first customer_id in the pairs of users and does not contain duplicates.

The code is tested on data sets provided by Amazon: https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz (about 667 MB). To analyze the reviews in the file, 4 worker nodes must be started.
