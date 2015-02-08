# filterOutliers
Simple Hadoop map/reduce chain to filter outliers in a list of users to counts.
Run mvn -u clean package, so you can execute the jar with two parameters, input file and an output folder.
Input file must be in the format specified by sample.csv.
Output folder will contain a file called filtered.csv.

This program will eliminate all lines/users from the input file whose counts are outside of the normal distribution aka are outliers.
A record is an outlier if the count is smaller than (mean - 5*standard_deviation), or larger than (mean + 5*standard_deviation).

TODO update the description.
