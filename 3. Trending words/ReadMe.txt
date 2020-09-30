//This ReadMe document describes how to run the code for trending word extraction
//The implementation is using SparkRDD and the package has the same structure as the assignment 3

To Run the code, run the TrendingWord-1.0-SNAPSHOT.jar file under /target. The input file folder should be named as 0.data. The parent directory will be passed as the args[0]. The output will be saved in the same directory

Sample commend line(for local excaution):
spark-submit --class edu.cs.cs226.htian003.TrendingWord --master local[*] target/TrendingWord-1.0-SNAPSHOT.jar file:///home/hao/Documents/  

We attached a small sample data (0.data) as input file in the submission
