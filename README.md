SquareDetector
==============

SquareDetector is a Java implementation of an algorithm for detecting complete bipartite subgraph _K_(2,2) (squares) in undirected graphs using Hadoop MapReduce.

## Setting up

Before running the program, create `squarecounter/input` directory and move your input files to Hadoop Distributed File System (HDFS). Your output files will be available in the `squarecounter/output` directory.

To run your program you have to compile the SquareDetector.java source file and create a squaredetector.jar file. Run the application in Hadoop using the following command (first `cd` to the directory where Hadoop is installed):

```bin/hadoop jar /<absolute-path>/squaredetector.jar SquareDetector squarecounter/input squarecounter/output```

where `<absolute-path>` is the directory path from root where your `squaredetector.jar` file is located.

For technical support and information, feel free to contact me to andrea.benedetti AT ymail DOT com.
