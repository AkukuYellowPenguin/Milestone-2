from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2ID70-MS2")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    return SparkContext.getOrCreate(spark_conf)

def row_flatmap(x):
    elements = x.split(",")
    attr = elements[1].split(';')
    num = elements[2].split(';')
    l = []

    for i in range(0,5):
        l.append(elements[0] + ',' + attr[i] + ',' + num[i])

    
    return l

def make_key(x):
    #get first and second column of row
    first_column = x.split(",")[0]
    second_column = x.split(",")[1]
    
    #combine first and second column into one string
    l = []
    for i in range(len(first_column)):
        l.append(first_column + "." + second_column)
        
    #return key element
    return l[0]

def get_sorted(x):
    list(set(x))
    x.sort()
    return ",".join(x)

def q1(spark_context: SparkContext, on_server) -> RDD:
    database_file_path = "/Database.csv" if on_server else "Database.csv"

    # TODO: You may change the value for the minPartitions parameter (template value 160) when running locally.
    # It is advised (but not compulsory) to set the value to 160 when running on the server.
    database_rdd = spark_context.textFile(database_file_path, 160)

    # TODO: Implement Q1 here by defining q1RDD based on databaseRDD.
    header = database_rdd.first()
    q1_rdd = database_rdd.filter(lambda x: x != header)
    q1s_rdd = q1_rdd.filter(lambda x: x[0] == 'S')
    s = q1s_rdd.count()
    q1t_rdd = q1_rdd.filter(lambda x: x[0] == 'T')
    t = q1t_rdd.count()
    q1r_rdd = q1_rdd.filter(lambda x: x[0] == 'R')
    r =  q1r_rdd.count()
    print(">> [q1: R: " + str(r) + "]")
    print(">> [q1: S: " + str(s) + "]")
    print(">> [q1: T: " + str(t) + "]")

    return q1_rdd


def q2(spark_context: SparkContext, q1_rdd: RDD):
    spark_session = SparkSession(spark_context)

    # TODO: Implement Q2 here.


def q3(spark_context: SparkContext, q1_rdd: RDD):
    # TODO: Implement Q3 here.

    #create rdd with first two columns as key
    q3_rdd = q1_rdd.map(lambda x: (make_key(x), [x.split(",")[2]]))
    #gather all values per key
    q3_rdd_reduced = q3_rdd.reduceByKey(lambda x, y: x + y)

    #switch key with values
    q3_rdd_switched = q3_rdd_reduced.map(lambda x : (get_sorted(x[1]), [x[0]]))
    #look for keys that are the same and gather only elements with value list size of > 1
    q3_rdd_result = q3_rdd_switched.reduceByKey(lambda x, y: x + y).filter(lambda x: len(x[1]) > 1) 
    result = q3_rdd_result.collect()

    #print result in correct format
    for i in result:
        print(">> [q3: " + i[1][0]+ "," + i[1][1] + "]")



def q4(spark_context: SparkContext, on_server):
    streaming_context = StreamingContext(spark_context, 2)
    streaming_context.checkpoint("checkpoint")

    hostname = "stream-host" if on_server else "localhost"
    lines = streaming_context.socketTextStream(hostname, 9000)

    # TODO: Implement Q4 here.
    total_address_count = lines.window(20,4).count()
    distinct_address = lines.map(lambda address: (address, 1))
    distinct_address_count = distinct_address.reduceByKeyAndWindow(lambda a1, a2: a1+a2, lambda a1, a2: a1-a2, 20, 4)
    address_frequency = distinct_address_count.transformWith(lambda a1, a2: a1.cartesian(a2), total_address_count).map(lambda x: (x[0][0], x[0][1]/x[1]))
    result = address_frequency.filter(lambda freq: 0.03 < freq[1])
    result.foreachRDD(lambda x: x.foreach(lambda y: print(f">> [q4: {y[0][0]}, {y[0][1]}]")))

    # Start the streaming context, run it for two minutes or until termination
    streaming_context.start()
    streaming_context.awaitTerminationOrTimeout(2 * 60)
    streaming_context.stop()


# Main 'function' which initializes a Spark context and runs the code for each question.
# To skip executing a question while developing a solution, simply comment out the corresponding function call.
if __name__ == '__main__':

    on_server = True  # TODO: Set this to true if and only if running on the server

    spark_context = get_spark_context(on_server)

    q1_rdd = q1(spark_context, on_server)

    # q2(spark_context, q1_rdd)

    # q3(spark_context, q1_rdd)

    # q4(spark_context, on_server)

    spark_context.stop()
