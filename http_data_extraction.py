from pyspark import SparkConf, SparkContext
import base64
from http_parsing.extractor import CountingFeatureExtractor


extractor = CountingFeatureExtractor("./features.json")
print(extractor.all_feature_names())
filename = "input/rapid7data/*.gz"
conf = SparkConf().setAppName("decodefiles").setMaster("local[4]")
sc = SparkContext(conf=conf)
data = sc.textFile(filename)
data = data.map(lambda x: tuple(x.split(","))).map(lambda x: (base64.b64decode(x[0]), x[1]))
extracted = data.map(lambda x: (extractor.accumulate_features_from_string(x[0].decode("utf-8")), x[1])).take(1)

print(extracted)


sc.stop()