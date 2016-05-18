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