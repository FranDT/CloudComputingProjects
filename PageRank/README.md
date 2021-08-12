# MapReduce PageRank
We have implemented two different version of the PageRank algorithm exploiting Hadoop and Spark.

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

## Spark
In this section, we want to show the implementation of the PageRank algorithm through Spark. In particular, we’ve used Python and Java for our implementation.

### Design of the algorithm in Python
In the following picture, we can see the DAG for the Python implementation of PageRank
through Spark, in which we show three iterations of PageRank (one is performed in the node initialization phase):

<img width="860" alt="Screen Shot 2021-08-12 at 11 01 01" src="https://user-images.githubusercontent.com/41535744/129169404-333845a7-602a-4299-ae85-d8bc77af4cb8.png">

* Count phase: We count the elements, obtaining our number of pages N.
* Node initialization phase: we append the initial rank equal to 1/N during the map phase to each page; after that, we calculate the contribution of each page and we obtain the sum of the contribution for each page, that is finally used to obtain, for each page, the new rank.
* Rank phase: we take the result of the previous iteration and calculate the new ranks.
* Sort phase: we use the sortBy transformation to obtain an RDD that is sorted by value, that corresponds to the rank.

### Design of the algorithm in Java
In the following picture, we’ve reported the DAG of the Java implementation, in which we show just two iterations of the PageRank algorithm:

<img width="731" alt="Screen Shot 2021-08-12 at 12 16 53" src="https://user-images.githubusercontent.com/41535744/129180435-546d8a7e-cecd-40f3-bf00-acdf3c07cc7f.png">

* Parse + Count phase: we perform both the parse and the count at the same time. The final RDD contains the title of each page and its initial rank equal to 1/N.
* Rank phase: we calculate, for each page, its new rank, that is then used in the following iteration.
* Sort phase: We perform the sorting.

### Spark execution
To execute the Spark code, we need to have the /SparkPageRank/pagerank.py file on the namenode. Once the file is on the namenode and we performed the access to the namenode itself, we just need to run the following command:
```
spark-submit <inputFile> <outputPath> <alpha> <numIteration>
```
To check the result, from the namenode we need to run the following command:
```
hadoop fs -cat <outputPath>/part-00000
```
