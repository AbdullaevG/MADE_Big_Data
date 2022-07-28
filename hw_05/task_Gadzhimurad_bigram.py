import re

from pyspark import SparkConf, SparkContext


def get_bigrams(text: str):
    """
    return bigrams of text. 
    Example:
    input: "How are you filling today?"
    output: ['how_are', 'are_you', 'you_filling', 'filling_today']
    """
    lower_text = re.findall("\w+", text.lower())
    
    return [word1 + "_" + word2 for word1, word2 in zip(lower_text[:-1], lower_text[1:])]



conf = SparkConf().setAppName("count_bigramms")
sc = SparkContext(conf=conf)

data_rdd = sc.textFile("hdfs:///data/wiki/en_articles_part").repartition(8)

data_rdd = (
    data_rdd
    .map(lambda x: x.split("\t")[1])
    .flatMap(get_bigrams)
    .filter(lambda x: x.startswith("narodnaya"))
    .map(lambda x: (x, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortByKey()
)

for bigramm, count in data_rdd.collect():
    print(bigramm, count, sep="\t")
