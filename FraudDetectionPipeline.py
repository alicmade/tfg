import time


class FraudDetectionPipeline:
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path
        self.df = None
        #self.model = None
        self.models = {}
        self.metrics = []

    def load_data(self):
        import os
        csv_file = [f for f in os.listdir(self.path) if f.endswith(".csv")][0]
        self.df = self.spark.read.csv(os.path.join(self.path, csv_file), header=True, inferSchema=True)
        print("ya ha cargado el dataset")
        self.df.show()
        self.df.printSchema()
        fraud = self.df.filter("isFraud = 1")
        non_fraud = self.df.filter("isFraud = 0").sample(fraction=0.0015, seed=42)  # ajusta la fracción

        balanced_df = fraud.union(non_fraud)

        self.df.groupBy("isFraud").count().show()
        self.df = balanced_df
        self.df.groupBy("isFraud").count().show()

    def preprocess(self):
        from pyspark.sql.functions import col
        from pyspark.ml.feature import StringIndexer, VectorAssembler

        # Añade la columna objetivo 'label'
        self.df = self.df.withColumn("label", col("isFraud").cast("integer"))

        # Codifica la columna 'type' (es string pero relevante)
        indexer = StringIndexer(inputCol="type", outputCol="type_indexed")
        self.df = indexer.fit(self.df).transform(self.df)

        # Excluye manualmente las columnas string no útiles
        exclude_cols = ["isFraud", "label", "type", "nameOrig", "nameDest"]

        # Toma solo columnas numéricas más la columna codificada
        cols = [c.name for c in self.df.schema.fields
                if str(c.dataType) != "StringType" and c.name not in exclude_cols] + ["type_indexed"]

        # Vectoriza
        assembler = VectorAssembler(inputCols=cols, outputCol="features")
        self.df = assembler.transform(self.df).select("features", "label")
        from pyspark import StorageLevel

        self.df.persist(StorageLevel.DISK_ONLY)

        print("ya se ha preprocesado el dataset")

#probar con mas modelos
    def train_and_evaluate_models(self):
        from pyspark.ml.classification import (
            RandomForestClassifier,
            LogisticRegression,
            GBTClassifier,
            DecisionTreeClassifier
        )
        from pyspark.ml.evaluation import BinaryClassificationEvaluator

        train, test = self.df.randomSplit([0.7, 0.3], seed=42)
        evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

        classifiers = {
            "RandomForest": RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100),
            "LogisticRegression": LogisticRegression(labelCol="label", featuresCol="features", maxIter=20),
            "GBTClassifier": GBTClassifier(labelCol="label", featuresCol="features", maxIter=50),
            "DecisionTree": DecisionTreeClassifier(labelCol="label", featuresCol="features")
        }

        for name, clf in classifiers.items():
            start_time = time.time()
            model = clf.fit(train)
            predictions = model.transform(test)
            auc = evaluator.evaluate(predictions)
            elapsed = time.time() - start_time
            self.models[name] = model
            self.metrics.append({
                "model": name,
                "AUC": round(auc, 4),
                "time_seconds": round(elapsed, 2)
            })
            print(f"{name} AUC: {auc:.4f} | Tiempo: {elapsed:.2f} s")

"""     def train_model(self):
        from pyspark.ml.classification import RandomForestClassifier
        train, test = self.df.randomSplit([0.7, 0.3], seed=42)
        #train = train.coalesce(2)
        #test = test.coalesce(2)
        rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)
        self.model = rf.fit(train)
        print("ya se ha el entrenamiento el dataset")
    def evaluate(self):
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        _, test = self.df.randomSplit([0.7, 0.3], seed=42)
        predictions = self.model.transform(test)
        evaluator = BinaryClassificationEvaluator(labelCol="label")
        auc = evaluator.evaluate(predictions)
        print(f"AUC: {auc:.4f}") """



