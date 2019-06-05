library(sparklyr)

sc <- spark_connect(master = "local")

#spark_read_csv(sc, "q3_2016", path = "/home/data_Q3_2016")
q4_2016 <- spark_read_csv(sc, "q4_2016", path = "E:/NSDB/sparkHardDrive/Q4_2016")

pred <- ml_predict(lr_model, q4_2016)

ml_binary_classification_evaluator(pred)

###############################################################################

# Cluster

sc <- spark_connect(master = "spark://master:7077",
                    version = "2.4",
                    spark_home = "/usr/local/spark")


# Retrieve the Spark installation directory
spark_home <- "/usr/local/spark/"

# Build path to start-master.sh
start_all <- file.path(spark_home, "sbin", "start-all.sh")

# Execute start-master.sh to start the cluster
system2(start_all)

# Build path to stop-master.sh
stop_all <- file.path(spark_home, "sbin", "stop-all.sh")

# Execute stop-all.sh to stop the cluster


#Finally we can disconect all connections
spark_disconnect(sc)
