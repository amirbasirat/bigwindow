# Building and running

    $ cd hadoop-springbatch-invertedindex
    $ mvn clean package appassembler:assemble
    $ sh ./target/appassembler/bin/invertedindex

The file in the local directory, data/urlwords.txt will be loaded 
into HDFS under the directory /user/activsteps/springbatch/invertedindex/input

The inverted index MapReduce job will put its results in the directory
/user/activsteps/springbatch/invertedindex/output

You can view the output by using the hadoop command line utility

    $ hadoop fs -cat /user/activsteps/springbatch/invertedindex/output



