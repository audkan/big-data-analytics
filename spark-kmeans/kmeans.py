# Import libraries
from pyspark import SparkContext
import sys

# Euclidean distance
def sqDist(p1, p2):
	# X-coordinate is at index 0, Y-coordinate is at index 1
	# (p1x-p2x)^2 + (p1y-p2y)^2
	return (p1[0]-p2[0])**2 + (p1[1]-p2[1])**2

def closestK(p, K):
	bestK = 0
	# Set closest to maximum float (infinity)
	closest = float("+inf")
	# Find the minimum distance between the point and a cluster center
	for i in range(len(K)):
		tempDist = sqDist(p, K[i])
		if tempDist < closest:
			closest = tempDist
			bestK = i
	# Return the INDEX of the best cluster center
	return bestK

def avgPoints(pts):
        summedXY = sum(i[0] for i in pts), sum(i[1] for i in pts)
        N = float(len(pts))
        return float(summedXY[0])/N, float(summedXY[1])/N
                                                
if __name__ == "__main__":
	if len(sys.argv) != 3:
		print>>sys.stderr, "Usage: KMeans.py <input> <output>"
		exit(-1)

	sc = SparkContext()
	# Grab coordinates from column 4 and 5
	# Parse strings to float and change (u'x',u'y') to (x,y)
	# Filter out empty coordinates
	datapoints = sc.textFile(sys.argv[1]) \
		.map(lambda line: line.split(",")) \
		.map(lambda w: (float(w[3].encode("utf-8")), float(w[4].encode("utf-8")))) \
		.filter(lambda xy: xy != (0,0)) \
		.persist()
	
	k = 5
	# Choose k random points as starting cluster centers
	kPoints = datapoints.takeSample(False, k, 34)

	convergeDist = 0.1
	tempDist = 1.0
	# If the center has changed by more than the convergeDist threshold, iterate again.
	# Otherwise, terminate.
	while tempDist > convergeDist:
		# Find all points closest to each cluster center
		closest = datapoints.map(lambda xy: (closestK(xy, kPoints), (xy)))
		# Group by closest cluster center
		# points = (k-index, Iterable[(x,y),(x,y),(x,y),(x,y)])
		points = closest.groupByKey().sortBy(lambda kxy: kxy[0])
		# Find the new center (mean) of each cluster
		newPoints = points.map(lambda kxy: (kxy[0], avgPoints(kxy[1])))

		# newPoints = (k-index, (avgX, avgY))
		# Sum the distances between the current and new centers of each cluster
		tempDist = sum( \
			newPoints.map(lambda kxy: sqDist(kPoints[kxy[0]], kxy[1])) \
			.collect())
		
		# E.g. newPoints = [(0, (x, y)), (1, (x, y)), (2, (x, y)), (3, (x, y)), (4, (x, y))]
		kPoints = newPoints.map(lambda kxy: kxy[1]).collect()

	clusterCenters = sc.parallelize(kPoints).coalesce(1).map(lambda line: str(line))
	clusterCenters.saveAsTextFile(sys.argv[2])
	sc.stop()


