#!/bin/bash
hdfs dfs -rm -r /user/ameir/task5_output 2>/dev/null
mapred streaming -files /home/ameir/se446_milestone1/src/mapper_task5.py,/home/ameir/se446_milestone1/src/reducer_sum.py -mapper "python3 mapper_task5.py" -reducer "python3 reducer_sum.py" -input /data/chicago_crimes_sample.csv -output /user/ameir/task5_output
hdfs dfs -cat /user/ameir/task5_output/part-00000 | head