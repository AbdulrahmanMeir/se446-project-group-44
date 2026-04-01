# SE446 – Big Data Engineering  
## Milestone 1: Chicago Crime Analytics with MapReduce

**Repository:** `se446-project-group-44`  
**Group Number:** 44  

---

## Team Members

| Name | Student ID | GitHub Username | Assigned Task |
|---|---:|---|---|
| Abdulrahman Meir | [210450] | [AbdulrahmanMeir] | [Milestone_1] |




---

## Executive Summary

This project implements four Hadoop MapReduce analytics tasks on the Chicago Crimes dataset using Python Hadoop Streaming. The goal is to answer key law-enforcement questions by processing a large crime dataset on the Hadoop cluster. The implemented tasks are: crime type distribution, location hotspots, crime trends by year, and arrest status analysis. Each task uses a custom Python mapper and a shared reducer to aggregate counts. The jobs were first tested on the sample dataset and then executed on the full dataset `/data/chicago_crimes.csv` for final submission. The repository contains the source code, execution scripts, and output files for all required tasks.

---

## Project Structure

```text
se446-project-group-44/
├── README.md
├── src/
│   ├── mapper_task2.py
│   ├── mapper_task3.py
│   ├── mapper_task4.py
│   ├── mapper_task5.py
│   └── reducer_sum.py
├── scripts/
│   ├── run_task2.sh
│   ├── run_task3.sh
│   ├── run_task4.sh
│   └── run_task5.sh
├── output/
│   ├── task2.txt
│   ├── task3.txt
│   ├── task4.txt
│   └── task5.txt
└── test_data.csv

Environment and Execution Notes

The jobs were executed on the Hadoop cluster using Hadoop Streaming.
For the final submission, the input dataset used is:
/data/chicago_crimes.csv

Before running any task on the cluster, the Hadoop environment must be loaded:
source /etc/profile.d/hadoop.sh

Task 2 – Crime Type Distribution
Research Question

What are the most common types of crimes in Chicago?

Logic

This task reads the Primary Type field at index 5 from each CSV row and emits (crime_type, 1).
The reducer sums the values for each crime type to get the total number of occurrences.

Exact Hadoop Streaming Command

mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task2.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task2.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task2

Top 5 Output Lines

THEFT	162688
BATTERY	151930
NARCOTICS	74127
ASSAULT	54070
BURGLARY	39872


Interpretation

Theft is the most frequent crime category in the full Chicago crime dataset, followed by battery and narcotics, which indicates these categories represent the largest share of recorded incidents.

Full Execution Log

ameir@master-node:~/se446_milestone1$ bash scripts/run_task2.sh
Running Task 2: Crime Type Distribution
Input: /data/chicago_crimes.csv
HDFS Output: /user/ameir/project/m1/task2
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob8068888269789433965.jar tmpDir=null
2026-04-01 18:48:07,499 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 18:48:07,787 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 18:48:08,243 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ameir/.staging/job_1771402826595_0271
2026-04-01 18:48:10,132 INFO mapred.FileInputFormat: Total input files to process : 1
2026-04-01 18:48:10,175 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-04-01 18:48:10,176 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-04-01 18:48:10,803 INFO mapreduce.JobSubmitter: number of splits:2
2026-04-01 18:48:11,762 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0271
2026-04-01 18:48:11,762 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-04-01 18:48:12,138 INFO conf.Configuration: resource-types.xml not found
2026-04-01 18:48:12,138 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-04-01 18:48:12,285 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0271
2026-04-01 18:48:12,352 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0271/
2026-04-01 18:48:12,354 INFO mapreduce.Job: Running job: job_1771402826595_0271
2026-04-01 18:48:30,142 INFO mapreduce.Job: Job job_1771402826595_0271 running in uber mode : false
2026-04-01 18:48:30,144 INFO mapreduce.Job:  map 0% reduce 0%
2026-04-01 18:49:00,467 INFO mapreduce.Job:  map 100% reduce 0%
2026-04-01 18:49:17,452 INFO mapreduce.Job:  map 100% reduce 100%
2026-04-01 18:49:20,297 INFO mapreduce.Job: Job job_1771402826595_0271 completed successfully
2026-04-01 18:49:20,597 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=11798790
                FILE: Number of bytes written=24540719
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=690
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=109020
                Total time spent by all reduces in occupied slots (ms)=26364
                Total time spent by all map tasks (ms)=54510
                Total time spent by all reduce tasks (ms)=13182
                Total vcore-milliseconds taken by all map tasks=54510
                Total vcore-milliseconds taken by all reduce tasks=13182
                Total megabyte-milliseconds taken by all map tasks=27909120
                Total megabyte-milliseconds taken by all reduce tasks=6749184
        Map-Reduce Framework
                Map input records=793074
                Map output records=793072
                Map output bytes=10212640
                Map output materialized bytes=11798796
                Input split bytes=198
                Combine input records=0
                Combine output records=0
                Reduce input groups=34
                Reduce shuffle bytes=11798796
                Reduce input records=793072
                Reduce output records=34
                Spilled Records=1586144
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=786
                CPU time spent (ms)=10790
                Physical memory (bytes) snapshot=683343872
                Virtual memory (bytes) snapshot=6559277056
                Total committed heap usage (bytes)=348168192
                Peak Map Physical memory (bytes)=256221184
                Peak Map Virtual memory (bytes)=2184708096
                Peak Reduce Physical memory (bytes)=176779264
                Peak Reduce Virtual memory (bytes)=2192257024
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=690
2026-04-01 18:49:20,597 INFO streaming.StreamJob: Output directory: /user/ameir/project/m1/task2

Top 5 results:
THEFT   162688
BATTERY 151930
NARCOTICS       74127
ASSAULT 54070
BURGLARY        39872

Full sorted output saved to: /home/ameir/se446_milestone1/output/task2.txt



Task 3 – Location Hotspots
Research Question

Where do most crimes occur?

Logic

This task reads the Location Description field at index 7 from each CSV row and emits (location_description, 1).
The reducer sums the values to identify the locations with the highest crime counts.

Exact Hadoop Streaming Command

mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task3.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task3.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task3

  Top 5 Output Lines

STREET	248326
RESIDENCE	136393
APARTMENT	61235
SIDEWALK	47506
OTHER	29671

Interpretation

The highest number of crimes occurred on streets and in residential locations, showing that public roads and living areas are the main crime hotspots in the dataset.

Full Execution Log

ameir@master-node:~/se446_milestone1$ bash scripts/run_task3.sh
Running Task 3: Location Hotspots
Input: /data/chicago_crimes.csv
HDFS Output: /user/ameir/project/m1/task3
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob6349979358985037503.jar tmpDir=null
2026-04-01 18:51:21,846 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 18:51:22,191 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 18:51:22,693 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ameir/.staging/job_1771402826595_0272
2026-04-01 18:51:24,455 INFO mapred.FileInputFormat: Total input files to process : 1
2026-04-01 18:51:24,486 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-04-01 18:51:24,487 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-04-01 18:51:25,113 INFO mapreduce.JobSubmitter: number of splits:2
2026-04-01 18:51:26,017 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0272
2026-04-01 18:51:26,017 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-04-01 18:51:26,361 INFO conf.Configuration: resource-types.xml not found
2026-04-01 18:51:26,362 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-04-01 18:51:26,472 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0272
2026-04-01 18:51:26,535 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0272/
2026-04-01 18:51:26,537 INFO mapreduce.Job: Running job: job_1771402826595_0272
2026-04-01 18:51:44,386 INFO mapreduce.Job: Job job_1771402826595_0272 running in uber mode : false
2026-04-01 18:51:44,388 INFO mapreduce.Job:  map 0% reduce 0%
2026-04-01 18:52:13,598 INFO mapreduce.Job:  map 50% reduce 0%
2026-04-01 18:52:14,853 INFO mapreduce.Job:  map 100% reduce 0%
2026-04-01 18:52:30,743 INFO mapreduce.Job:  map 100% reduce 100%
2026-04-01 18:52:33,596 INFO mapreduce.Job: Job job_1771402826595_0272 completed successfully
2026-04-01 18:52:33,827 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=12719805
                FILE: Number of bytes written=26382749
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=4761
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=106010
                Total time spent by all reduces in occupied slots (ms)=27074
                Total time spent by all map tasks (ms)=53005
                Total time spent by all reduce tasks (ms)=13537
                Total vcore-milliseconds taken by all map tasks=53005
                Total vcore-milliseconds taken by all reduce tasks=13537
                Total megabyte-milliseconds taken by all map tasks=27138560
                Total megabyte-milliseconds taken by all reduce tasks=6930944
        Map-Reduce Framework
                Map input records=793074
                Map output records=791479
                Map output bytes=11136841
                Map output materialized bytes=12719811
                Input split bytes=198
                Combine input records=0
                Combine output records=0
                Reduce input groups=212
                Reduce shuffle bytes=12719811
                Reduce input records=791479
                Reduce output records=212
                Spilled Records=1582958
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=797
                CPU time spent (ms)=11710
                Physical memory (bytes) snapshot=705253376
                Virtual memory (bytes) snapshot=6561787904
                Total committed heap usage (bytes)=348127232
                Peak Map Physical memory (bytes)=272519168
                Peak Map Virtual memory (bytes)=2186846208
                Peak Reduce Physical memory (bytes)=179900416
                Peak Reduce Virtual memory (bytes)=2190491648
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=4761
2026-04-01 18:52:33,833 INFO streaming.StreamJob: Output directory: /user/ameir/project/m1/task3

Top 5 results:
STREET  248326
RESIDENCE       136393
APARTMENT       61235
SIDEWALK        47506
OTHER   29671

Full sorted output saved to: /home/ameir/se446_milestone1/output/task3.txt


Task 4 – Crime Trends by Year
Research Question

How has the total number of crimes changed over the years?

Logic

This task reads the Date field at index 2, extracts the year from the date string, and emits (year, 1).
The reducer sums the counts to produce the total number of crimes per year.

Exact Hadoop Streaming Command
mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task4.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task4.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task4

  Top 5 Output Lines

2001	467301
2002	205267
2003	985
2004	915
2005	1031


Interpretation

The yearly crime totals show that the dataset contains a very large number of records in 2001 and 2002, while the following years in this output have much smaller counts, indicating the records are unevenly distributed across years.

Full Execution Log

ameir@master-node:~/se446_milestone1$ bash scripts/run_task4.sh
Running Task 4: Crime Trends by Year
Input: /data/chicago_crimes.csv
HDFS Output: /user/ameir/project/m1/task4
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob15298079048803516270.jar tmpDir=null
2026-04-01 19:58:59,588 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 19:58:59,953 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 19:59:00,437 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ameir/.staging/job_1771402826595_0275
2026-04-01 19:59:02,124 INFO mapred.FileInputFormat: Total input files to process : 1
2026-04-01 19:59:02,159 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-04-01 19:59:02,160 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-04-01 19:59:02,781 INFO mapreduce.JobSubmitter: number of splits:2
2026-04-01 19:59:03,702 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0275
2026-04-01 19:59:03,702 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-04-01 19:59:04,014 INFO conf.Configuration: resource-types.xml not found
2026-04-01 19:59:04,014 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-04-01 19:59:04,122 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0275
2026-04-01 19:59:04,177 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0275/
2026-04-01 19:59:04,179 INFO mapreduce.Job: Running job: job_1771402826595_0275
2026-04-01 19:59:22,055 INFO mapreduce.Job: Job job_1771402826595_0275 running in uber mode : false
2026-04-01 19:59:22,057 INFO mapreduce.Job:  map 0% reduce 0%
2026-04-01 19:59:51,914 INFO mapreduce.Job:  map 100% reduce 0%
2026-04-01 20:00:08,187 INFO mapreduce.Job:  map 100% reduce 100%
2026-04-01 20:00:11,166 INFO mapreduce.Job: Job job_1771402826595_0275 completed successfully
2026-04-01 20:00:11,536 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=7137663
                FILE: Number of bytes written=15218468
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=245
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=108228
                Total time spent by all reduces in occupied slots (ms)=24186
                Total time spent by all map tasks (ms)=54114
                Total time spent by all reduce tasks (ms)=12093
                Total vcore-milliseconds taken by all map tasks=54114
                Total vcore-milliseconds taken by all reduce tasks=12093
                Total megabyte-milliseconds taken by all map tasks=27706368
                Total megabyte-milliseconds taken by all reduce tasks=6191616
        Map-Reduce Framework
                Map input records=793074
                Map output records=793073
                Map output bytes=5551511
                Map output materialized bytes=7137669
                Input split bytes=198
                Combine input records=0
                Combine output records=0
                Reduce input groups=25
                Reduce shuffle bytes=7137669
                Reduce input records=793073
                Reduce output records=25
                Spilled Records=1586146
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=731
                CPU time spent (ms)=9840
                Physical memory (bytes) snapshot=650633216
                Virtual memory (bytes) snapshot=6562951168
                Total committed heap usage (bytes)=348098560
                Peak Map Physical memory (bytes)=253431808
                Peak Map Virtual memory (bytes)=2185498624
                Peak Reduce Physical memory (bytes)=148275200
                Peak Reduce Virtual memory (bytes)=2192580608
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=245
2026-04-01 20:00:11,536 INFO streaming.StreamJob: Output directory: /user/ameir/project/m1/task4


First 5 lines:
2001    467301
2002    205267
2003    985
2004    915
2005    1031

Full sorted output saved to: /home/ameir/se446_milestone1/output/task4.txt


Task 5 – Arrest Analysis
Research Question

What percentage of crimes result in an arrest?

Logic

This task reads the Arrest field at index 8 and emits (arrest_status, 1).
The reducer sums the values for true and false to compare arrest outcomes.

Exact Hadoop Streaming Command

mapred streaming \
  -files /home/ameir/se446-project-group-44/src/mapper_task5.py,/home/ameir/se446-project-group-44/src/reducer_sum.py \
  -mapper "python3 mapper_task5.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/ameir/project/m1/task5

   Top 5 Output Lines

false	571140
true	221932

Interpretation

Most crime records in the dataset did not result in an arrest, while a smaller but still significant portion resulted in true arrest status.

Full Execution Log

ameir@master-node:~/se446_milestone1$ bash scripts/run_task5.sh
Running Task 5: Arrest Analysis
Input: /data/chicago_crimes.csv
HDFS Output: /user/ameir/project/m1/task5
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob9748564533859171471.jar tmpDir=null
2026-04-01 19:00:08,584 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 19:00:09,002 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-04-01 19:00:09,583 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/ameir/.staging/job_1771402826595_0274
2026-04-01 19:00:11,754 INFO mapred.FileInputFormat: Total input files to process : 1
2026-04-01 19:00:11,807 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-04-01 19:00:11,808 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-04-01 19:00:12,503 INFO mapreduce.JobSubmitter: number of splits:2
2026-04-01 19:00:13,508 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0274
2026-04-01 19:00:13,509 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-04-01 19:00:13,903 INFO conf.Configuration: resource-types.xml not found
2026-04-01 19:00:13,903 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-04-01 19:00:14,040 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0274
2026-04-01 19:00:14,119 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0274/
2026-04-01 19:00:14,122 INFO mapreduce.Job: Running job: job_1771402826595_0274
2026-04-01 19:00:33,394 INFO mapreduce.Job: Job job_1771402826595_0274 running in uber mode : false
2026-04-01 19:00:33,396 INFO mapreduce.Job:  map 0% reduce 0%
2026-04-01 19:00:56,482 INFO mapreduce.Job:  map 100% reduce 0%
2026-04-01 19:01:09,792 INFO mapreduce.Job:  map 100% reduce 100%
2026-04-01 19:01:12,626 INFO mapreduce.Job: Job job_1771402826595_0274 completed successfully
2026-04-01 19:01:12,873 INFO mapreduce.Job: Counters: 55
        File System Counters
                FILE: Number of bytes read=7708794
                FILE: Number of bytes written=16360727
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=25
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters
                Killed map tasks=1
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=84896
                Total time spent by all reduces in occupied slots (ms)=22054
                Total time spent by all map tasks (ms)=42448
                Total time spent by all reduce tasks (ms)=11027
                Total vcore-milliseconds taken by all map tasks=42448
                Total vcore-milliseconds taken by all reduce tasks=11027
                Total megabyte-milliseconds taken by all map tasks=21733376
                Total megabyte-milliseconds taken by all reduce tasks=5645824
        Map-Reduce Framework
                Map input records=793074
                Map output records=793072
                Map output bytes=6122644
                Map output materialized bytes=7708800
                Input split bytes=198
                Combine input records=0
                Combine output records=0
                Reduce input groups=2
                Reduce shuffle bytes=7708800
                Reduce input records=793072
                Reduce output records=2
                Spilled Records=1586144
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=598
                CPU time spent (ms)=7250
                Physical memory (bytes) snapshot=645373952
                Virtual memory (bytes) snapshot=6562004992
                Total committed heap usage (bytes)=347971584
                Peak Map Physical memory (bytes)=249581568
                Peak Map Virtual memory (bytes)=2184876032
                Peak Reduce Physical memory (bytes)=147009536
                Peak Reduce Virtual memory (bytes)=2193498112
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=25
2026-04-01 19:01:12,879 INFO streaming.StreamJob: Output directory: /user/ameir/project/m1/task5

Output lines:
false   571140
true    221932

Full sorted output saved to: /home/ameir/se446_milestone1/output/task5.txt