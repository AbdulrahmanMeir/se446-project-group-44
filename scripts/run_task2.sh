#!/bin/bash
hdfs dfs -rm -r /user/ameir/task2_output 2>/dev/null
mapred streaming -files /home/ameir/se446_milestone1/src/mapper_task2.py,/home/ameir/se446_milestone1/src/reducer_sum.py -mapper "python3 mapper_task2.py" -reducer "python3 reducer_sum.py" -input /data/chicago_crimes_sample.csv -output /user/ameir/task2_output
hdfs dfs -cat /user/ameir/task2_output/part-00000 | head