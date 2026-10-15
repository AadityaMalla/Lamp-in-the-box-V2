# =============================================================
# Author : Aaditya Malla, Khadija Osman, Natoli Mayu
# File   : spark/server.py
# Purpose: Spark SQL service backed by a Delta Lake table stored
#          in an AWS S3 bucket. Exposes a small JSON API that the
#          Rocket (Rust) container calls.
# =============================================================
import glob
import json
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import pyspark
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ.get("DELTA_BUCKET", "csci312-delta-aaditya-2026")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
TABLE_PATH = f"s3a://{BUCKET}/delta/students"
SCHEMA = "id INT, name STRING, major STRING, gpa DOUBLE, year INT"

SEED = [
    (1, "Alice", "Computer Science", 3.8, 3),
    (2, "Bob", "Mathematics", 3.2, 2),
    (3, "Carol", "Physics", 3.6, 4),
    (4, "David", "Computer Science", 2.9, 1),
    (5, "Emma", "Biology", 3.9, 3),
    (6, "Frank", "Mathematics", 3.5, 4),
    (7, "Grace", "Computer Science", 3.4, 2),
    (8, "Henry", "Physics", 2.7, 1),
    (9, "Isla", "Biology", 3.1, 2),
    (10, "Jack", "Computer Science", 3.7, 4),
]


def hadoop_version():
    """hadoop-aws must match the Hadoop version bundled inside PySpark."""
    jars = os.path.join(os.path.dirname(pyspark.__file__), "jars")
    jar = glob.glob(os.path.join(jars, "hadoop-client-api-*.jar"))[0]
    return re.search(r"hadoop-client-api-(.+)\.jar", jar).group(1)


def build_spark():
    builder = (
        SparkSession.builder.appName("delta-lake-service")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.port", "4040")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3 access; credentials come from the EC2 instance's IAM role
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    )
    packages = [f"org.apache.hadoop:hadoop-aws:{hadoop_version()}"]
    return configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()


def ensure_table(spark):
    """Create the Delta table in S3 with sample data on first run."""
    if not DeltaTable.isDeltaTable(spark, TABLE_PATH):
        print(f"Creating Delta table at {TABLE_PATH}", flush=True)
        spark.createDataFrame(SEED, SCHEMA).write.format("delta").mode("overwrite").save(TABLE_PATH)
    else:
        print(f"Using existing Delta table at {TABLE_PATH}", flush=True)


def query_students(spark, major=None, min_gpa=None, version=None):
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)        # time travel
    reader.load(TABLE_PATH).createOrReplaceTempView("students")

    sql = "SELECT id, name, major, gpa, year FROM students WHERE 1 = 1"
    args = {}
    if major:
        sql += " AND lower(major) = lower(:major)"              # named parameter,
        args["major"] = major                                   # never string-pasted
    if min_gpa is not None:
        sql += " AND gpa >= :min_gpa"
        args["min_gpa"] = min_gpa
    sql += " ORDER BY id"

    rows = spark.sql(sql, args=args or None).collect()
    return {
        "table": TABLE_PATH,
        "version": version if version is not None else "latest",
        "sql": sql,
        "params": args,
        "count": len(rows),
        "rows": [r.asDict() for r in rows],
    }


def add_student(spark, rec):
    df = spark.read.format("delta").load(TABLE_PATH)
    new_id = (df.agg(F.max("id")).collect()[0][0] or 0) + 1
    row = [(new_id, str(rec["name"]), str(rec["major"]), float(rec["gpa"]), int(rec["year"]))]
    spark.createDataFrame(row, SCHEMA).write.format("delta").mode("append").save(TABLE_PATH)
    return {"added_id": new_id, "table": TABLE_PATH}


def table_history(spark):
    hist = (DeltaTable.forPath(spark, TABLE_PATH).history()
            .select("version", "timestamp", "operation").orderBy(F.desc("version")).collect())
    return {"table": TABLE_PATH,
            "history": [{"version": h["version"], "timestamp": str(h["timestamp"]),
                         "operation": h["operation"]} for h in hist]}


spark = None


class Handler(BaseHTTPRequestHandler):
    def send_json(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        try:
            if url.path == "/health":
                self.send_json(200, {"status": "ok"})
            elif url.path == "/students":
                major = q.get("major", [None])[0]
                min_gpa = float(q["min_gpa"][0]) if "min_gpa" in q else None
                version = int(q["version"][0]) if "version" in q else None
                self.send_json(200, query_students(spark, major, min_gpa, version))
            elif url.path == "/history":
                self.send_json(200, table_history(spark))
            else:
                self.send_json(404, {"error": "not found"})
        except ValueError:
            self.send_json(400, {"error": "invalid parameter"})
        except Exception as e:
            self.send_json(500, {"error": str(e)[:300]})

    def do_POST(self):
        try:
            if urlparse(self.path).path != "/students":
                return self.send_json(404, {"error": "not found"})
            length = int(self.headers.get("Content-Length", 0))
            rec = json.loads(self.rfile.read(length))
            self.send_json(201, add_student(spark, rec))
        except (ValueError, KeyError, TypeError):
            self.send_json(400, {"error": "invalid student record"})
        except Exception as e:
            self.send_json(500, {"error": str(e)[:300]})

    def log_message(self, fmt, *args):
        print("spark-api:", fmt % args, flush=True)


if __name__ == "__main__":
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)
    print("Delta Lake service listening on :9000", flush=True)
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
