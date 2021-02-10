from pyspark import SparkConf, SparkContext
import sys
import json

sc = SparkContext.getOrCreate()   

input_file = sys.argv[1]
output_file = sys.argv[2]

# input_file = 'Gamergate.json'
# output_file = 'task2_output.json'

raw_data = sc.textFile(input_file)
dataset = raw_data.map(json.loads)
# dataset.persist()

retweets = dataset.map(lambda tweet: tweet['retweet_count'])
# retweets.persist()

retweets_stats = retweets.stats()
# print(retweets_stats)

# TASK 2 A
mean_retweet = retweets_stats.mean()
# print(mean_retweet)

# TASK 2 B
max_retweet = retweets_stats.max()
# print(max_retweet)

# TASK 2 C
stdev_retweet = retweets_stats.stdev()
# print(stdev_retweet)

solution = {}
solution['mean_retweet'] = mean_retweet
solution['max_retweet'] = max_retweet
solution['stdev_retweet'] = stdev_retweet
solution_string = json.dumps(solution)
out_file = open(output_file, "w")  
out_file.write(solution_string)
out_file.close()  

sc.stop()