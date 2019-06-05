library(sparklyr)
library(tidyverse)

sc <- spark_connect(master = "local")

#features for regression:
#smart_1_normalized, smart_3_normalized, smart_5_normalized,
#smart_7_normalized, smart_9_normalized, smart_192_normalized,
#smart_193_normalized, smart_194_normalized

## logistic regression
sample_tbl <- sdf_copy_to(sc, sample, name = "sample", overwrite = TRUE)

partitions <- sample_tbl %>%
  sdf_partition(training = 0.7, test = 0.3, seed = 1111)

sample_training <- partitions$training
sample_test <- partitions$test

lr_model <- sample_training %>%
  ml_logistic_regression(failure ~ smart_1_normalized + smart_3_normalized + smart_5_normalized + smart_7_normalized + smart_9_normalized) # + smart_192_normalized + smart_193_normalized + smart_194_normalized

pred <- ml_predict(lr_model, sample_test)

ml_binary_classification_evaluator(pred)


# local application ###########################################################

q4_2016 <- spark_read_csv(sc, "q4_2016", path = "E:/NSDB/sparkHardDrive/Q4_2016")

pred <- ml_predict(lr_model, q4_2016)
ml_binary_classification_evaluator(pred)

