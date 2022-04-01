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
    return False
    # TODO: Implement Q3 here.


def q4(spark_context: SparkContext, on_server):
    streaming_context = StreamingContext(spark_context, 2)
    streaming_context.checkpoint("checkpoint")

    hostname = "stream-host" if on_server else "localhost"
    lines = streaming_context.socketTextStream(hostname, 9000)

    # TODO: Implement Q4 here.

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
