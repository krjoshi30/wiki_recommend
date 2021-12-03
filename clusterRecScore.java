import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.*;
import java.io.*;
import java.lang.Math;

public class clusterRecScore {

	public static class Mapper3 extends Mapper<Object, Text, Text, Text> {

                private Text node1;
                private Text node2;

                @Override
                protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        while (itr.hasMoreTokens()) {
                                String s1 = itr.nextToken();
                                String s2 = itr.nextToken();
                                node1 = new Text(s1);
                                node2 = new Text(s2);
                                context.write(node1,node2);
                        }
                }
        }

	public static class Reducer3 extends Reducer<Text, Text, Text, NullWritable> {

                private TreeMap<String, String> treeMap;
                private TreeMap<String, String> shortestPaths;
                private List<String> list1;
                private List<String> list2;

                @Override
                protected void setup(Context context) throws IOException, InterruptedException {
                        shortestPaths = new TreeMap<String,String>();

                        URI[] cacheFiles = context.getCacheFiles();

                        if (cacheFiles != null && cacheFiles.length > 0) {
                                try {
                                     	String line = "";
                                        FileSystem fs = FileSystem.get(context.getConfiguration());
                                        Path getFilePath = new Path(cacheFiles[0].toString());
                                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                                        while ((line = reader.readLine()) != null) {
                                                String[] words = line.split(" ");
                                                        shortestPaths.put(words[0],words[1]);
                                        }
                                }
                                catch (Exception e) {
                                        System.out.println("Unable to read the File");
                                        System.exit(1);
                                }
                        }
			treeMap = new TreeMap<String,String>();
                        list1 = new ArrayList<String>();
                        list2 = new ArrayList<String>();
		}

                @Override
                protected void reduce(Text key, Iterable<Text> values, Context context) {
                        String s = "";
                        for (Text val : values) {
                                String s1 = String.valueOf(val.toString());
                                s = s + s1 + ",";
                        }
                        s = s.substring(0, s.length() - 1);
                        treeMap.put(key.toString(),s);
                }
		@Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
                        String s = "";
                        int count = 0;
                        int sum = 0;
                        double averageLength = 0.00;
                        double totalAverage = 0.00;
                        String s2 = "";
                        String s3 = "";
                        String s4 = "";
                        String s5 = "";
			String s6 = "";
			String s7 = "";
                        String temp = "";
                        int totalCount = 0;

                        Set<Map.Entry<String,String>> entries3 = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries2 = shortestPaths.entrySet();

			for(Map.Entry<String,String> entry : entries) {
                                count = 0;
				s = entry.getValue();
                                if(s.contains(",")) {
                                        for(int i = 0; i < s.length(); i++) {
                                                if(s.charAt(i) == ',') {
                                                        count++;
                                                }
                                        }
                                        for(int j = 0; j < count; j++) {
                                                if(s.contains(",")) {
                                                        s2 = s.substring(0,s.indexOf(","));
                                                        if(s2.equals("none")) {}
							else {
								list1.add(s2);
                                                        }
							s = s.substring(s.indexOf(",") + 1,s.length());
                                                }
                                        }
					if(s.equals("none")) {}
                                        else {
                                       		list1.add(s);
                                     	}
					list1.add(entry.getKey());
				}
				else {
					if(s.equals("none")) {
						list1.add(entry.getKey());
					}
					else {
						list1.add(entry.getKey());
						list1.add(entry.getValue());
					}
				}
                                count = 0;


				for(int m = 0; m < list1.size(); m++) {
					for(Map.Entry<String,String> entry2 : entries2) {
                                             	if(entry2.getKey().equals(list1.get(m))) {
                                               		s5 = entry2.getValue();
                                                      	break;
                                             	}
                                      	}
					for(int n = 0; n < list1.size(); n ++) {
						if(m != n) {
							s6 = s5.substring(0,Integer.parseInt(list1.get(n))-1);
							s7 = s5.substring(Integer.parseInt(list1.get(n)),s5.length());
							s5 = s6 + '9' + s7;
						}

					}
					context.write(new Text(list1.get(m) + " " + s5),NullWritable.get());
				}
				list1.clear();
			}
                }
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, NullWritable> {
                private TreeMap<String,String> treeMap;

                @Override
                protected void setup(Context context) throws IOException, InterruptedException {
                        treeMap = new TreeMap<String,String>();
                }

		@Override
                protected void reduce(Text key, Iterable<Text> values, Context context) {
                        String s = "";
                        for (Text val : values) {
                                String s1 = val.toString();
                                s = s + s1 + ",";
                        }
                        s = s.substring(0, s.length() - 1);
                        treeMap.put(key.toString(),s);
                }

		@Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
                        String s = "";
                        String s1 = "";
                        String s2 = "";
                        double sum = 0.00;
			char temp = '0';

                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();

                        for(Map.Entry<String,String> entry : entries) {
                                s = entry.getValue();
				s1 = "";
				for(int i = 0; i < s.length(); i++) {
					temp = s.charAt(i);
					if(temp == '_' || temp == '9' || temp == '0') {
						if(temp == '_') {
                                                	sum = 0.10;
                                                	s1 = s1 + Double.toString(sum) + ",";
                                        	}
                                        	else if(temp == '9') {
                                                	sum = 0.75;
                                                	s1 = s1 + Double.toString(sum) + ",";
                                        	}
                                        	else {
                                                	sum = 0.00;
                                                	s1 = s1 + Double.toString(sum) + ",";
                                        	}
					}
					else {
						int x = temp - '0';
						sum = 1.00 - ((x - 1)*(1.00))/((10 - 1)*(1.00));
						s1 = s1 + Double.toString(sum) + ",";
					}
				}
				s1 = s1.substring(0, s1.length() - 1);
				context.write(new Text(entry.getKey() + " " + s1),NullWritable.get());
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf6 = new Configuration();
                Job job6 = Job.getInstance(conf6, "Job6");
                job6.setJarByClass(clusterRecScore.class);
                job6.setMapperClass(clusterRecScore.Mapper3.class);
                job6.setReducerClass(clusterRecScore.Reducer3.class);
                job6.setNumReduceTasks(1);
                job6.setMapOutputKeyClass(Text.class);
                job6.setMapOutputValueClass(Text.class);
                job6.setOutputKeyClass(Text.class);
                job6.setOutputValueClass(NullWritable.class);
                job6.addCacheFile(new URI("/input/shortest-path-distance-matrix.txt"));
                FileInputFormat.addInputPath(job6, new Path(args[0]));
                FileOutputFormat.setOutputPath(job6, new Path(args[1]));
		job6.waitForCompletion(true);

		Configuration conf2 = new Configuration();
                Job job2 = Job.getInstance(conf2, "Job2");
                job2.setJarByClass(clusterRecScore.class);
                job2.setMapperClass(clusterRecScore.Mapper3.class);
                job2.setReducerClass(clusterRecScore.Reducer2.class);
                job2.setNumReduceTasks(1);
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(NullWritable.class);
                FileInputFormat.addInputPath(job2, new Path(args[1]));
                FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}
}
