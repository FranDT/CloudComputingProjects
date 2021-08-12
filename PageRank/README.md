# MapReduce PageRank
We have implemented two different version of PageRank algorithm exploiting Hadoop and Spark. The Spark version is implemented both in Java and Python. The Hadoop version is implemented using Java.

In the following sections, you will have a simple guide on the project implementation. Major details can be found in the documentation.

## Hadoop
The following picture represents the sequence of phases that the input file has to go through when processed with our algorithm:

<img width="892" alt="Screen Shot 2021-08-12 at 10 28 52" src="https://user-images.githubusercontent.com/41535744/129164598-afb79a9a-6345-4261-9b34-aafb8081c9b8.png">

* Count phase: during the count phase, we retrieve the number of pages.
* Parse phase: we initialize all the Nodes, with an initial rank equal to 1 divided by our computed number of pages.
* Rank Phase: the aim of this phase is to take, for each node, the rank calculated in the previous phase and to divide this rank by the number of outlinks, so that we can represent the contribution of the page with respect to each outlink
* Sort phase: during the last phase, we perform the sorting.

### Hadoop Execution
To execute the Hadoop implementation of the PageRank algorithm, it is necessary to create
a jar file to be run on the cluster: to do this, we first need to clone the GitHub repository, then we need to run the following command inside the /HadoopPageRank folder:
```
mvn clean package
```
After this, the jar file will be created with the name “HadoopPageRank-1.0-SNAPSHOT.jar” and will be placed inside /HadoopPageRank/target. If it is not performed inside the namenode, we should move the jar file to the namenode. Now that we have our jar file on the namenode, we can access to the it at IP
hadoop@172.16.3.207. To test the algorithm, we need to run the following command:
```
hadoop jar <jarPath> it.unipi.hadoop.Main <inputFile> <outputPath> <numIteration> <alpha> <numReducers>
```
To check the result, from the namenode we need to run the following command:
```
hadoop fs -cat <outputPath>/sort/part-r-00000
```
