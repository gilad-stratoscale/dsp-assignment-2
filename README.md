# Word Relatedness - Assignment 2 in Distributed System Programming course at BGU

The assignment in the course website: https://www.cs.bgu.ac.il/~dsp162/Assignments/Assignment_2

- To develop comfortably, install local hadoop. a good tutorial **for mac**:
http://zhongyaonan.com/hadoop-tutorial/setting-up-hadoop-2-6-on-mac-osx-yosemite.html
- A nice tutorial to run your first mapreduce program can be found in hadoop's website:
https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

## Example application
The app is separated to 2 modules: the local module and the mapreduce module.
They have a mutual parent.
In order to run the app (currently a wordcount app):
- Install the maven project (`mvn install`)
- Locate the jar that was created by maven
- Run hadoop: `hadoop jar <jar-path> <wordcount-hdfs-input-path> <wordcount-hdfs-output-path>`
    For example:
    `hadoop jar /Users/dsp-assignment-2/dsp-assignment-2-mapreduce/target/dsp-assignment-2-mapreduce-1.0-SNAPSHOT-job.jar wordcount/input wordcount/output`

We are currently based on [hadoop official tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
and a [tikal tutorial](http://www.tikalk.com/build-your-first-hadoop-project-maven/) for maven + hadoop.

## Unit tests
- Use [MRUnit](https://mrunit.apache.org/) ([tutorials](https://cwiki.apache.org/confluence/display/MRUNIT/MRUnit+Tutorial)).

## Map-reduce steps
The map reduce steps are as follows:
1. Split the n-grams into 2-grams. In addition a simple word count app to count how many times each word and each 2-gram appears in each decade
2. grouping of each 2-gram count with its first word count, i.e. for the 2-gram "map reduce", we will write the result
"map,<map-count>,map reduce,<map-reduce-count" with the key "reduce".
In addition, in this step we will write each single word with its own count, i.e. for the word "map" we will write
"map,<map-count>" with "map" as a key
3. final calculation: Now we will get in the reducer, for the 2gram "map reduce" the count of "map", the count of
"reduce" and the count of "map reduce", so we are ready to caculate the PMI, and this is what we will do in this part.