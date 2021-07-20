from pyspark import SparkContext
import sys
import re

def createAdjElement(line):
    start = "<title>"
    end = "</title>"
    linkStartDelim = "[["
    linkEndDelim = "]]"
    
    key = line[line.find(start)+len(start):line.rfind(end)]
    links = []
    parts = line.split(linkStartDelim)
    links = re.findall("\\[\\[(.*?)\\]\\]", line)
    """
    for i in range(1, len(parts)):
        part = parts[i]
        value = part[0:part.find(linkEndDelim)]
        links.append(value)
    links = list(dict.fromkeys(links))
    """
    return (key, links)


def mainMap(record):
    returnedList = []
    outlinks = len(record[1])
    returnedList.append((record[0], 0))
    for outlink in record[1]:
        returnedList.append((outlink, record[2]/outlinks))
    return returnedList


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Please use the following syntax: pagerank.py <inputSource> <outputLocation> <alpha> <#iterations>")
        sys.exit(-1)
        
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    alpha = float(sys.argv[3])
    maxIter = int(sys.argv[4])
    
    master = "local[*]"
    appName = "PageRank"
    
    sc = SparkContext(master, appName)

    #Process input source into an RDD
    rdd = sc.textFile(inputFile)

    #Create Adjacency list as a map
    mapped = rdd.map(createAdjElement).cache()

    #Count the number of distinct nodes in the graph
    pages = mapped.map(lambda rec: rec[0]).distinct()
    print(pages.count())
    outlinks = mapped.flatMap(lambda rec: rec[1]).distinct()
    N = pages.union(outlinks).distinct().count()
    #N = pages + outlinks

    print("N is")
    print(N)
    print("\n\n\n\n")
    
    #Append the initial page rank to the adjacency list
    pageRanks = mapped.map(lambda rec: (rec[0], rec[1], 1/N))

    #Perform the initial MapReduce to compute the ranks
    flattenedContributions = pageRanks.flatMap(lambda rec: mainMap(rec))
    summedContributions = flattenedContributions.reduceByKey(lambda x, y: x + y)
    pageRanks = summedContributions.mapValues(lambda s: (alpha / N) +(float(1-alpha) * s))

    #Further MapReduce iterations
    for i in range (1, maxIter-1):
        joined = mapped.join(pageRanks)
        pageRanks = joined.map(lambda rec: (rec[0], rec[1][0], rec[1][1]))
        flattenedContributions = pageRanks.flatMap(lambda rec: mainMap(rec))
        summedContributions = flattenedContributions.reduceByKey(lambda x, y: x + y)
        pageRanks = summedContributions.mapValues(lambda s: (alpha / N) +(float(1-alpha) * s))

    sorted_page_ranks = pageRanks.sortBy(lambda page: page[1], ascending=False, numPartitions=1)

    sorted_page_ranks.saveAsTextFile(outputFile)
