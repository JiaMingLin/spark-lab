import numpy as np
from time import time
from itertools import combinations
from pyspark import SparkConf, SparkContext, SQLContext

from tools import Base

class DepGraphSeq(Base):
	"""
	"""
	def __init__(self, eps1 = 1.0, batch_size = 20, threh = 0.2):
		self.eps1 = eps1
		self.batch_size = batch_size
		self.threh = threh

	def fit(self, df):

		df.persist()
		self.nrow = df.count()
		
		batch = self.get_attrs_batch(df.columns)

		measures = dict()

		def dependency(pair):
			pandas_df = bxtab.value[pair]
			cols = pandas_df.columns[1:]
			mat = pandas_df.as_matrix(columns = cols)
			dl = mat.shape[0]; dk = mat.shape[1]
			expected_sum = self.get_expected_sum(mat)
			chi2 = np.sum((mat.T - expected_sum) ** 2 / expected_sum)
			CV = np.sqrt(chi2 / (nrow * (min(dk, dl) - 1)))
			return CV

		for i, pairs in enumerate(batch):
			start = time()
			xtables = [df.select(pair[0], pair[1]).coalesce(1).crosstab(pair[0], pair[1]) for pair in pairs]
			xtables_collected = [xtab.toPandas() for xtab in xtables]
			bxtab = sc.broadcast(dict(zip(pairs, xtables_collected)))

			CVs = sc.parallelize(pairs).map(lambda pair: len(bxtab.value[pair])).collect()
			CVs = zip(pairs, CVs)
			measures.update(CVs)
			print "#%d batch spends %2.f sec." % (i, time()-start)

		return self.pick_associations(measures)

	def pick_associations(self, pair_measure):
		return dict([ pm for pm in pair_measure.items() if pm[1] > self.threh])

	def get_attrs_batch(self, attrs):
		# drop those attributes with empty name
		attrs = [a for a in attrs if len(a) > 0 ]
		# the set of all pair attributes combinations
		comb_attrs = list(combinations(attrs, 2))
		stairs = np.arange(0, len(comb_attrs), step = self.batch_size)
		return [comb_attrs[left:left+self.batch_size] for left in stairs]

class DepGraphBatch(Base):
	def __init__(self, eps1 = 1.0, batch_size = 500, threh = 0.2):
		self.eps1 = eps1
		self.batch_size = batch_size
		self.threh = threh

	def fit(self, df):
		df = df.repartition(40)
		df.persist()
		self.nrow = df.count()

		features = [col for col in df.columns if len(col) > 0]
		ncols = len(features); index = range(ncols)
		
		batch = self.get_attrs_batch(index)

		for one_round in batch:
			pair_count = df.mapPartitions(lambda iterable: Base.pair_func_cnt(iterable, one_round)) \
						.repartitionAndSortWithinPartitions(
							self.batch_size, 
							lambda e: (((2 * ncols -1 -e[0]) * e[0]) / 2 + (e[1] - e[0]) -1 ) % self.batch_size
						) \
						.mapPartitions(Base.g_test)
			print pair_count.collect()

	def get_attrs_batch(self, attrs):
		# drop those attributes with empty name
		attrs = [a for a in attrs if len(str(a)) > 0 ]
		# the set of all pair attributes combinations
		comb_attrs = list(combinations(attrs, 2))
		stairs = np.arange(0, len(comb_attrs), step = self.batch_size)
		return [comb_attrs[left:left+self.batch_size] for left in stairs]

class DepGraphBroadCast(Base):
	pass


def get_args(argv):
	return argv[0], argv[1], argv[2]

if __name__ == "__main__":

	import sys

	data_path, eps, com_type = get_args(sys.argv[1:])

	conf = (SparkConf()
			.setAppName("Generate Data")
			.set("spark.executor.memory", "512m")
			.set("spark.driver.memory", "512m"))

	sc = SparkContext(conf = conf, pyFiles=['tools.py'])
	sqlContext = SQLContext(sc)
	df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(data_path)

	b = time()
	if com_type == 'seq':
		graph = DepGraphSeq(eps1 = float(eps))
		
		result = graph.fit(df)
		
		print result
	elif com_type == 'batch':
		graph = DepGraphBatch(eps1 = float(eps))
		graph.fit(df)
	print "Total time %d" % (time() - b)
	sc.stop()