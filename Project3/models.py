from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def train_random_forest(train_data):
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
    model = rf.fit(train_data)
    return model

def train_gbt(train_data):
    gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=100)
    model = gbt.fit(train_data)
    return model

def evaluate(model, test_data):
    predictions = model.transform(test_data)
    metrics = {}
    for metric_name in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric_name
        )
        score = evaluator.evaluate(predictions)
        metrics[metric_name] = score
    return metrics
