echo "Removing old output"

$HADOOP_HOME/bin/hadoop fs -test -e /wiki_recommend/output/cosine
if [ $? = 0 ]; then
  $HADOOP_HOME/bin/hadoop fs -rm -r /wiki_recommend/output/cosine
fi

echo "Running job"

#	$SPARK_HOME/bin/spark-submit cosine_distance.py /wiki_recommend/test_input/ /wiki_recommend/output/cosine true
$SPARK_HOME/bin/spark-submit cosine_distance.py /wiki_recommend/medium_input/ /wiki_recommend/output/cosine true
#	$SPARK_HOME/bin/spark-submit cosine_distance.py /wiki_recommend/input/ /wiki_recommend/output/cosine true

if [ $? -ne 0 ]; then
  exit 1
fi

echo "Downloading output"

$HADOOP_HOME/bin/hadoop fs -getmerge /wiki_recommend/output/cosine/ ./cosine.csv
