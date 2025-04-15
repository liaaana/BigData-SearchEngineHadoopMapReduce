from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
import re

spark = SparkSession.builder \
    .appName("data preparation") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 1000 
df = df.select(['id', 'title', 'text']).orderBy(rand(seed=42)).limit(n) # better selection

files_saved = 0
def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    # check if text is not empty and does not have xml or json structure
    if len(row['text'].strip()) > 0 and not re.search(r'<[^>]+>', row['text']) and not re.match(r'^\{.*\}$', row['text']):
        global files_saved
        with open(filename, "w", encoding='utf-8') as f:
            f.write(row['text'].strip().replace('\t', ' '))
            files_saved += 1
        print(f"saved #{files_saved}: {filename}")
    else:
        print(f"skipped: {filename}")


df.foreach(create_doc)

df.write.csv("/index/data", sep="\t")