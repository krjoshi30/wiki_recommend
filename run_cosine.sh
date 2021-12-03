# Check for the right number of arguments and that they are valid

#if [ "$#" -ne 3 ]; then
#	echo "Usage: bash recompile_and_run.sh <cosine> <inputPath> <outputPath> <between0and1>"
#	exit 1
#fi

# ##########################################################################################################################################################

	echo "Removing old output"

	$HADOOP_HOME/bin/hadoop fs -test -e /wiki_recommend/output/cosine 
	if [ $? = 0 ]; then 
		$HADOOP_HOME/bin/hadoop fs -rm -r /wiki_recommend/output/cosine	
	fi

	echo "Running job"

	$SPARK_HOME/bin/spark-submit cosine_distance.py /wiki_recommend/input/ /wiki_recommend/output/cosine true
	
	if [ $? -ne 0 ]; then 
		exit 1 
	fi

	echo "Downloading output"

	rm -r ./cosineOutput

	$HADOOP_HOME/bin/hadoop fs -get /wiki_recommend/output/cosine/ ./cosineOutput
	mv ./cosineOutput/*.csv ./cosineOutput/cosine.csv
