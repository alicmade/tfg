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
        #from pyspark import StorageLevel

       #self.df.persist(StorageLevel.DISK_ONLY)

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
            # Guardar el modelo si es necesario

    def save_metrics_to_csv(self, file_path="resultados_metricas.csv"):
        import csv
        import os
        print("ya se ha guardado el metrics")
        # Verificamos si hay métricas
        if not self.metrics:
            print("No metrics to save.")
            return
        fieldnames = self.metrics[0].keys()

        # Guardamos las métricas en un CSV
        with open(file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for metric in self.metrics:
                writer.writerow(metric)

        print(f"Metrics saved to {os.path.abspath(file_path)}")


    def upload_csv_to_google_sheets(self, csv_path, sheet_url, creds_path="credenciales.json"):


                print("Iniciando carga de datos en Google Sheets...")
                try:
                    import csv, gspread
                    from oauth2client.service_account import ServiceAccountCredentials
                    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
                    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
                    client = gspread.authorize(creds)
                    print("Autenticación completada")
                except Exception as e:
                    print(f"Error en autenticación: {e}")
                    return

                try:
                    spreadsheet = client.open_by_url(sheet_url)
                    worksheet = spreadsheet.get_worksheet(0)
                    print("Hoja abierta correctamente")
                except Exception as e:
                    print(f"Error al abrir la hoja: {e}")
                    return

                try:
                    with open(csv_path, 'r') as f:
                        reader = csv.reader(f)
                        data = list(reader)

                    worksheet.update("A1", data)
                    print("Datos subidos correctamente a Google Sheets")
                except Exception as e:
                    print(f"Error al subir los datos: {e}")

    def export_results(self, csv_path="resultados_metricas.csv", sheet_url=None, creds_path="./credenciales.json"):

            self.save_metrics_to_csv(sheet_url, csv_path)
            if sheet_url:
                print("Subiendo resultados a Google Sheets..")
                self.upload_csv_to_google_sheets(csv_path, sheet_url, creds_path)



