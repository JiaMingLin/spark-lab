import numpy as np
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("yarn")
         .setAppName("Generate Data")
         .set("spark.executor.memory", "2g")
         .set("spark.driver.memory", "5g"))
sc = SparkContext(conf = conf)


output_dir = '/%s'
nlvs = [10, 20]
ncols = [5,10,15]
nrows = [10000,100000]

def gen_data(nlv, ncol, nrow ):
    header = np.asarray([['V%d' % ind for ind in range(ncol)]])

    data = np.random.randint(nlv, size=[nrow, ncol])
    data = np.concatenate((header, data), axis=0)
    
    rdd = sc.parallelize(data).map(lambda row: ','.join(list(row)))
    file_name = 'lv%d_col%d_row%d' % (nlv, ncol, nrow)
    rdd.saveAsTextFile(output_dir % file_name)

combs = [(nlv, ncol, nrow) for nlv in nlvs for ncol in ncols for nrow in nrows]
for (nlv, ncol, nrow) in combs:
    gen_data(nlv, ncol, nrow)