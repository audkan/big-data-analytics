# K-Means Algorithm

DeviceStatusETL.py* is a Python script that performs data scrubbing on the file *devicestatus.txt* to get it into a standardized format for later processing. The file *devicestatus.txt* contains data collected from mobile devices on Loudacre’s network, including device ID, current status, location and so on. The python script does the following things.
1. Load the dataset
2. Use the character at position 19 as the delimiter (since the 1st use of the delimiter is at position
19), parse the line, and filter out bad lines. Each line should have exactly 14 fields, so any line
that does not have 14 fields is considered as a bad line.
3. Extract the date ([0]), manufacturer ([1]),
device ID ([2]), and latitude and longitude ([12] and [13] respectively).
4. Save the extracted data to common delimited text files on HDFS.

---
`cd /home/cloudera/spark_training/data`

`hadoop fs -put devicestatus.txt loudacre/devicestatus.txt`

`spark-submit DeviceStatusETL.py loudacre/devicestatus.txt loudacre/devicestatus_etl`

---

## Calculate k-means for device location

This Spark application in Python implements a K-means algorithm to calculate K-means for the device location (i.e. latitude and longitude) in the file that is prepared by the previous step.


* Number of means (center points) K = 5
* convergeDist = 0.1 to decide when the K-means calculation is done
* Take a random sample of K location points as starting center points, using `takeSample(False, K, 34)`. For example, `rdd.takeSample(False, 5, 34)` takes a random sample of 5 location points from the RDD as starting center points and it returns an array of length 5, where “False” means no replacement, and 34 is the value of seed.
* Only includes known locations (i.e. filter out (0, 0) locations)
