from pyspark import SparkConf, SparkContext
import sys
import json

sc = SparkContext.getOrCreate()   

input_file = sys.argv[1]
output_file = sys.argv[2]

# input_file = 'Gamergate.json'
# output_file = 'task1_output.json'

raw_data = sc.textFile(input_file)
dataset = raw_data.map(json.loads)
# dataset.persist()

# TASK 1 A
n_tweet = dataset.count()
# print(n_tweet)

# TASK 1 B
users = dataset.map(lambda tweet: tweet['user'])
n_user = users.map(lambda user : user['id']).distinct().count()
# print(n_user)

# TASK 1 C
popular_users_data = users.map(lambda user: (user['screen_name'],user['followers_count']))
popular_users_data = popular_users_data.sortBy(lambda x : x[1], ascending=False)
popular_users = [[user[0], user[1]] for user in popular_users_data.take(3)]
# print(popular_users)

# TASK 1 D
Tuesday_Tweet=dataset.filter(lambda tweet: tweet['created_at'].startswith( "Tue" )).count()
# print(Tuesday_Tweet)

solution = {}
solution['n_tweet'] = n_tweet
solution['n_user'] = n_user
solution['popular_users'] = popular_users
solution['Tuesday_Tweet'] = Tuesday_Tweet
solution_string = json.dumps(solution)
out_file = open(output_file, "w")  
out_file.write(solution_string)
out_file.close()  

sc.stop()