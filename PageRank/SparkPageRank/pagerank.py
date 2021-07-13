from pyspark import SparkContext


def createAdjElement(line):
    start = "<title>"
    end = "</title>"
    linkStartDelim = "[["
    linkEndDelim = "]]"
    
    key = line[line.find(start)+len(start):line.rfind(end)]
    links = []
    parts = line.split(linkStartDelim)
    for i in range(1, len(parts)):
        part = parts[i]
        value = part[0:part.find(linkEndDelim)]
        links.append(value)

    return (key, links)


def mainMap(record):
    returnedList = []
    outlinks = len(record[1])
    returnedList.append((record[0], 0))
    for outlink in record[1]:
        returnedList.append((outlink, record[2]/outlinks))
    return returnedList

master = "local[*]"
appName = "PageRank"
maxIter = 3

sc = SparkContext(master, appName)

rdd = sc.textFile("/home/ahmed/Desktop/input.txt")
mapped = rdd.map(createAdjElement)

toBeCounted1 = mapped.flatMap(lambda rec: rec[1])
toBeCounted2 = mapped.flatMap(lambda rec: rec[0])

toBeCounted = toBeCounted1.union(toBeCounted2)

N = toBeCounted.distinct().count()

pageRanks = mapped.map(lambda rec: (rec[0], rec[1], 1/N))
flattenedContributions = pageRanks.flatMap(lambda rec: mainMap(rec))
y = flattenedContributions.keys().distinct().count()
print(N)
print(y)

summedContributions = flattenedContributions.reduceByKey(lambda x, y: x + y)
pageRanks = summedContributions.mapValues(lambda s: (float(1 - 0.8) / N) +(0.8 * float(s)))

for i in range (1, maxIter-1):
    joined = mapped.join(pageRanks)
    pageRanks = joined.map(lambda rec: (rec[0], rec[1][0], rec[1][1]))
    flattenedContributions = pageRanks.flatMap(lambda rec: mainMap(rec))
    summedContributions = flattenedContributions.reduceByKey(lambda x, y: x + y)
    pageRanks = summedContributions.mapValues(lambda s: (float(1 - 0.8) / N) +(0.8 * float(s)))

sorted_page_ranks = pageRanks.sortBy(lambda page: page[1])
y = sorted_page_ranks.min()

print(y)
