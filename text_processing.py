#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext
import sys
import json
import string

sc = SparkContext.getOrCreate()   

input_file = sys.argv[1]
output_file = sys.argv[2]

# input_file = 'tweets'
# output_file = 'task3_output.json'

raw_data = sc.textFile(input_file)

words = raw_data.flatMap(lambda line: line.split())
# words.persist()

words = words.map(lambda x: (x, 1))
word_counts = words.reduceByKey(lambda x, y: x+y)
# TASK 3 A
max_word_data = word_counts.takeOrdered(1, key=lambda x: -x[1])
max_word = [max_word_data[0][0], max_word_data[0][1]]
# print(max_word)

# TASK 3 B
mindless_count = word_counts.filter(lambda x : x[0]=='mindless').collect()[0][1]
# print(mindless_count)

# TASK 3 C
chunk_count = word_counts.filter(lambda x : x[0]=='|********************').collect()[0][1]
# print(chunk_count)

solution = {}
solution['max_word'] = max_word
solution['mindless_count'] = mindless_count
solution['chunk_count'] = chunk_count
solution_string = json.dumps(solution)
out_file = open(output_file, "w")  
out_file.write(solution_string)
out_file.close()  

sc.stop()