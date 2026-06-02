import glob
import os
import jpype
import time
import jpype.imports
from jpype import JImplements, JOverride

if not jpype.isJVMStarted():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    jars_path = os.path.join(base_dir, "lucene_jars", "*.jar")

    jars = glob.glob(jars_path)
    if not jars:
        print("Missing .jar files!")
    
    jpype.startJVM(classpath=jars, convertStrings=True)

from org.apache.lucene.store import NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.analysis.standard import StandardAnalyzer

from org.apache.lucene.search import IndexSearcher, DoubleValuesSource
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queries.function import FunctionScoreQuery
from java.util.function import LongToDoubleFunction
from org.apache.lucene.expressions import SimpleBindings
from org.apache.lucene.expressions.js import JavascriptCompiler

# overrided class to add in weights for engagement
def MultiplyConst(const):
    @JImplements(LongToDoubleFunction)
    class ScalerProxy(object):
        @JOverride
        def applyAsDouble(self, value):
            return float(value) * const
    return ScalerProxy()
    
def search(index_dir, query_str, field="post_text", top_k=10):
    store = NIOFSDirectory(Paths.get(index_dir))
    reader = DirectoryReader.open(store)
    searcher = IndexSearcher(reader)

    parser = QueryParser(field, StandardAnalyzer())
    query = parser.parse(query_str)

    wt_likes = DoubleValuesSource.fromField("num_likes", MultiplyConst(1.0))

    wt_reposts = DoubleValuesSource.fromField("num_reposts", MultiplyConst(4.0))

    wt_replies = DoubleValuesSource.fromField("num_replies", MultiplyConst(2.5))

    binding_vals = SimpleBindings()
    binding_vals.add("wt_likes", wt_likes)
    binding_vals.add("wt_reposts", wt_reposts)
    binding_vals.add("wt_replies", wt_replies)
    
    engagement_formula = "1.0 + wt_likes + wt_reposts + wt_replies"
    engagement_compile = JavascriptCompiler.compile(engagement_formula)
    engagement_score = engagement_compile.getDoubleValuesSource(binding_vals)

    curr_time = DoubleValuesSource.constant(time.time())
    query_time = DoubleValuesSource.fromField("time_secs", MultiplyConst(1.0))
    time_bindings = SimpleBindings()
    time_bindings.add("curr_time", curr_time)
    time_bindings.add("query_time", query_time)

    time_formula = "curr_time - query_time"
    time_compile = JavascriptCompiler.compile(time_formula)
    time_age_secs = time_compile.getDoubleValuesSource(time_bindings)

    # a post loses half its score in 72 hours
    scale = DoubleValuesSource.constant(86400.0 * 3.0)

    time_score_bindings = SimpleBindings()
    time_score_bindings.add("scale", scale)
    time_score_bindings.add("time_age_secs", time_age_secs)
    time_score_formula = "scale / (scale + time_age_secs)"
    time_score_compile = JavascriptCompiler.compile(time_score_formula)
    time_score = time_score_compile.getDoubleValuesSource(time_score_bindings)

    engagement_time_bindings = SimpleBindings()
    engagement_time_bindings.add("engagement_score", engagement_score)
    engagement_time_bindings.add("time_score", time_score)
    engagement_time_formula = "engagement_score * time_score"
    engagement_time_compile = JavascriptCompiler.compile(engagement_time_formula)
    engagement_time_score = engagement_time_compile.getDoubleValuesSource(engagement_time_bindings)

    enhanced_query = FunctionScoreQuery.boostByValue(query, engagement_time_score)
    hits = searcher.search(enhanced_query, top_k).scoreDocs

    results = []
    for hit in hits:
        doc = searcher.doc(hit.doc)
        results.append({
            "score":            round(hit.score, 4),
            "category":         doc.get("category"),
            "username":         doc.get("username"),
            "display_name":     doc.get("display_name"),
            "title":            doc.get("title"),
            "url":              doc.get("url"),
            "raw_time":         doc.get("raw_time"),
            "likes":            doc.get("num_likes"),
            "reposts":          doc.get("num_reposts"),
            "replies":          doc.get("num_replies"),
            "post_text":        doc.get("post_text")
        })
    reader.close()
    return results