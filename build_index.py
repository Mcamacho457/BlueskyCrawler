import os
import sys
import subprocess
import jpype, jpype.imports, glob
import shutil
import hashlib
import json
from datetime import datetime

# python script to build inverted index
# subprocess.run("sudo apt-get update -qq && sudo apt-get install -y -qq openjdk-17-jdk-headless > /dev/null",
#                shell=True,
#                check=True)

subprocess.run([sys.executable,
               "-m", "pip", "install", "--user", "--force-reinstall", "-q", "JPype1==1.5.2"],
               check=True)

os.makedirs("lucene_jars", exist_ok=True)
os.chdir("lucene_jars")

LUCENE = "8.11.3"
MAVEN = "https://repo1.maven.org/maven2/org/apache/lucene"

print("Downloading baseline ASM dependency JARs...")
# Replaces: !wget -q -nc ...
subprocess.run(["wget", "-q", "-nc", "https://repo1.maven.org/maven2/org/ow2/asm/asm/7.2/asm-7.2.jar"], check=True)
subprocess.run(["wget", "-q", "-nc", "https://repo1.maven.org/maven2/org/ow2/asm/asm-commons/7.2/asm-commons-7.2.jar"], check=True)

print("Downloading Lucene JAR modules...")
for mod in ["lucene-core", "lucene-analyzers-common", "lucene-queryparser", "lucene-queries", "lucene-expressions"]:
    download_url = f"{MAVEN}/{mod}/{LUCENE}/{mod}-{LUCENE}.jar"
    subprocess.run(["wget", "-q", "-nc", download_url], check=True)

subprocess.run(["wget", "-q", "-nc", "https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.5.1-1/antlr4-runtime-4.5.1-1.jar"], check=True)

subprocess.run("ls -lh *.jar", shell=True, check=True)

print("Changing back to main directory...")
os.chdir("/home/cs172/BlueskyCrawler")

if not jpype.isJVMStarted():
    jars = glob.glob("lucene_jars/*.jar")
    jpype.startJVM(classpath=jars, convertStrings=True)

from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, StoredField, IntPoint, StringField, NumericDocValuesField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader, Term
from org.apache.lucene.store import NIOFSDirectory
# hash function to create a unique post_id for each post
def make_post_id(text, timestamp):
    clean_text = str(text).strip()
    clean_text = " ".join(clean_text.split())

    clean_timestamp = str(timestamp).strip()
    
    post_str = f'{clean_text}||{clean_timestamp}'
    post_id = hashlib.md5(post_str.encode('utf-8')).hexdigest()

    return post_id

post_files = ["bluesky_clean_data/clean_conspiracy.json", "bluesky_clean_data/clean_paranormal.json", "bluesky_clean_data/clean_strange_earth.json", "bluesky_clean_data/clean_ufo.json"]
def create_index(index_dir, post_files):
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    os.makedirs(index_dir)

    store = NIOFSDirectory(Paths.get(index_dir))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    meta_type = FieldType()
    meta_type.setStored(True)
    meta_type.setTokenized(False)

    text_type = FieldType()
    text_type.setStored(True)
    text_type.setTokenized(True)
    text_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
    
    total_num_posts = 0
    for file in post_files:
        with open(file, "r", encoding="utf-8") as f:
            file_name = file.split("/")[-1]
            cat_with_format = file_name.split("_")[-1]
            category = cat_with_format.split(".")[0]
            for post in f:
                if not post.strip():
                    continue
                
                data_line = json.loads(post)

                post_title = data_line.get('title', "")
                username = data_line.get("user", "")
                display_name = data_line.get("display_name", "") or ""
                post_url = data_line.get("post_url", "")
                post_text = data_line.get("text", "")
                num_replies = data_line.get("reply_count", "")
                top_5_replies = data_line.get("top_5_replies", [])
                num_likes = data_line.get("like_count", "")
                num_reposts = data_line.get("repost_count", "")
                image_titles = data_line.get("image_titles", [])

                raw_time = data_line.get("time", "")
                raw_time = str(raw_time).strip() if raw_time else ""
                if raw_time == "":
                    time_secs = 0
                else:
                    try:
                        date_time = datetime.strptime(raw_time, "%m/%d/%Y %H:%M")
                        time_secs = int(date_time.timestamp())
                    except:
                        print(f"date string in wrong format: {raw_time}, setting to 0.")
                        time_secs = 0

                doc = Document()
                doc.add(Field("title",              post_title,        meta_type))
                doc.add(Field("username",           username,          meta_type))
                doc.add(Field("display_name",       display_name,      meta_type))
                doc.add(Field("url",                post_url,          meta_type))
                doc.add(Field("raw_time",           raw_time,          meta_type))
                doc.add(Field("category",           category,          meta_type))

                if isinstance(top_5_replies, list) and len(top_5_replies) > 0:
                    for reply in top_5_replies:
                        # prevents indexing replies that have no text 
                        reply = str(reply).strip()
                        if reply:
                            doc.add(Field("top_5_replies", reply, text_type))

                if isinstance(image_titles, list):
                    for img in image_titles:
                        doc.add(Field("image_titles", str(img), meta_type))
                else:
                    doc.add(Field("image_titles", str(image_titles), meta_type))

                doc.add(Field("post_text",          post_text,         text_type))

                # storing as a StoredField ensures we can also filter results by numerical value of these fields
                doc.add(StoredField("num_replies",  int(num_replies)))
                doc.add(StoredField("num_likes",    int(num_likes)))
                doc.add(StoredField("num_reposts",  int(num_reposts)))

                doc.add(IntPoint("num_replies",     int(num_replies)))
                doc.add(IntPoint("num_likes",       int(num_likes)))
                doc.add(IntPoint("num_reposts",     int(num_reposts)))

                doc.add(NumericDocValuesField("num_replies", int(num_replies)))
                doc.add(NumericDocValuesField("num_likes",   int(num_likes)))
                doc.add(NumericDocValuesField("num_reposts", int(num_reposts)))

                doc.add(NumericDocValuesField("time_secs", time_secs))

                post_id = make_post_id(post_text, raw_time)
                doc.add(StringField("post_id", post_id, Field.Store.YES))
                writer.updateDocument(Term("post_id", post_id), doc)
                total_num_posts += 1


    writer.commit()
    writer.close()

    print(f"✅ Indexed {total_num_posts} documents to {index_dir}")

INDEX_DIR = "bluesky_index"
create_index(INDEX_DIR, post_files)