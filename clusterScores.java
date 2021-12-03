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

public class clusterScores {

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

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

	public static class Reducer1 extends Reducer<Text, Text, Text, NullWritable> {
                private TreeMap<String,String> treeMap;
                @Override
                protected void setup(Context context) {
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
			String s2 = "";
			String s3 = "";
			String s4 = "";
			String s5 = "";
			double one = 0.00;
			double two = 0.00;
			double three = 0.00;
			Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries2 = treeMap.entrySet();
			for(Map.Entry<String,String> entry : entries) {
				s = entry.getKey();
				s2 = s.substring(0,s.indexOf("-"));
				s3 = s.substring(s.indexOf("-") + 1,s.length());
                                s4 = s3 + "-" + s2;
				for(Map.Entry<String,String> entry2 : entries2) {
					if(entry2.getKey().equals(s4)) {
						s5 = entry2.getValue();
						break;
					}
				}
				one = Double.parseDouble(entry.getValue());
				two = Double.parseDouble(s5);
				three = one + two;
                                context.write(new Text(s2 + " " + entry.getKey() + "-" + Double.toString(three)), NullWritable.get());
                        }
                }
        }

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
                        String temp = "";
                        int totalCount = 0;

                        Set<Map.Entry<String,String>> entries3 = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries2 = shortestPaths.entrySet();

			for(Map.Entry<String,String> entry : entries) {
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

				for(Map.Entry<String,String> entry3 : entries3) {
                                        if(!(entry3.getKey().equals(entry.getKey()))) {
                                                count = 0;
                                                s3 = entry3.getValue();
                                                if(s3.contains(",")) {
                                                        for(int i = 0; i < s3.length(); i++) {
                                                                if(s3.charAt(i) == ',') {
                                                                        count++;
                                                                }
                                                        }
                                                }
						else {
							if(s3.equals("none")) {
                                                		list2.add(entry3.getKey());
                                        		}
                                        		else {
                                                		list2.add(entry3.getKey());
                                                		list2.add(entry3.getValue());
                                        		}
						}
                                                if(count > 0) {
							for(int j = 0; j < count; j++) {
                                                        	if(s3.contains(",")) {
                                                                	s4 = s3.substring(0, s3.indexOf(","));
                                                                	if(s4.equals("none")) {}
                                                        		else {
                                                                		list2.add(s4);
                                                        		}
                                                                	s3 = s3.substring(s3.indexOf(",") + 1,s3.length());
                                                        	}
                                                	}
							if(s3.equals("none")) {}
                                                        else {
								list2.add(s3);
							}
							list2.add(entry3.getKey());
						}
                                        }
					for(int m = 0; m < list1.size(); m++) {
                                                temp = "";
                                                sum = 0;
                                                for(Map.Entry<String,String> entry2 : entries2) {
                                                        if(entry2.getKey().equals(list1.get(m))) {
                                                                s5 = entry2.getValue();
                                                                break;
                                                        }
                                                }
                                                for(int n = 0; n < list2.size(); n++) {
                                                        int x = Integer.parseInt(list2.get(n)) - 1;
                                                        char c = s5.charAt(x);
                                                        temp = temp + c;
                                                }
                                                for(int v = 0; v < temp.length(); v++) {
                                                        if(temp.charAt(v) == '_') {
                                                                sum = sum + 3;
                                                        }
                                                        else {
                                                              	int q = temp.charAt(v) - '0';
                                                                sum = sum + q;
                                                        }
                                                }
                                                int z = temp.length();
                                                averageLength = (1.00*sum)/(1.00*z);
                                                totalAverage = totalAverage + averageLength;
                                                totalCount++;
                                                temp = "";
                                                sum = 0;

                                        }
					double total = totalAverage/(1.00*totalCount);
                                        if(entry.getKey().equals(entry3.getKey())) {}
                                        else {
                                                context.write(new Text(entry.getKey() + "-" + entry3.getKey() + " "  + Double.toString(total)),NullWritable.get());
                                        }
                                        list2.clear();
                                        temp = "";
                                        sum = 0;
                                        totalAverage = 0.00;
                                        totalCount = 0;
                                }
                                list2.clear();
                                list1.clear();
                                count = 0;
                        }
                }
	}

	public static class Reducer2 extends Reducer<Text, Text, Text, NullWritable> {
                private TreeMap<String,String> treeMap;
                private List<String> list;
                private TreeMap<String,String> clusters;


                @Override
                protected void setup(Context context) throws IOException, InterruptedException {
                        clusters = new TreeMap<String,String>();

                        URI[] cacheFiles = context.getCacheFiles();

                        if (cacheFiles != null && cacheFiles.length > 0) {
                                try {
                                     	String line = "";
                                        FileSystem fs = FileSystem.get(context.getConfiguration());
                                        Path getFilePath = new Path(cacheFiles[0].toString());
                                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                                        while ((line = reader.readLine()) != null) {
                                                String[] words = line.split(" ");
                                                        clusters.put(words[0],words[1]);
                                        }
                                }
                                catch (Exception e) {
                                        System.out.println("Unable to read the File");
                                        System.exit(1);
                                }
                        }

                        treeMap = new TreeMap<String,String>();
                        list = new ArrayList<String>();
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
                        double max = 0.00;
                        double min = 20.00;
                        int count = 0;
                        String s3 = "";
                        String s4 = "";
                        String s5 = "";
                        String s6 = "";
                        String s7 = "";
                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries2 = clusters.entrySet();
                        for(Map.Entry<String,String> entry : entries) {
                                s = entry.getValue();

                                for(int i = 0; i < s.length(); i++) {
                                        if(s.charAt(i) == ',') {
                                                count++;
                                        }
                                }
                                for(int j = 0; j < count; j++) {
                                        s1 = s.substring(0, s.indexOf(","));
                                        list.add(s1);
                                        s = s.substring(s.indexOf(",") + 1,s.length());
                                }
                                list.add(s);

				for(int k = 0; k < list.size(); k++) {
                                        s3 = list.get(k);
                                        int x = s3.lastIndexOf("-");
                                        //context.write(new Text("Page: " + s3 + "Index: " + Integer.toString(x)),NullWritable.get());
                                        s2 = s3.substring(x + 1,s3.length());
                                        sum = Double.parseDouble(s2);
                                        if(sum < min) {
                                                min = sum;
                                        }
                                        if(sum > max) {
                                                max = sum;
                                        }
                                }
                                for(int l = 0; l < list.size(); l++) {
                                        s3 = list.get(l);
                                        int x = s3.lastIndexOf("-");
                                        //context.write(new Text("Page: " + s3 + "Index: " + Integer.toString(x)),NullWritable.get());
                                        sum = Double.parseDouble(s3.substring(x + 1,s3.length()));
                                        sum = 1 - ((sum - min)/(max - min));
                                        String fin = list.get(l).substring(0,x);
                                        int y = fin.lastIndexOf("-");
                                        s4 = fin.substring(0,y);
                                        s5 = fin.substring(y + 1,fin.length());
                                        for(Map.Entry<String,String> entry2 : entries2) {
                                                if(entry2.getKey().equals(s4)) {
                                                        s6 = entry2.getValue() + "," + entry2.getKey();
                                                }
                                                if(entry2.getKey().equals(s5)) {
                                                        s7 = entry2.getValue() + "," + entry2.getKey();
                                                }
                                        }

					context.write(new Text(s4 + "-" + s6 + "-" + s5 + "-" + s7 + " " + Double.toString(sum)),NullWritable.get());
                                }
                                list.clear();
                                count = 0;
                                sum = 0.00;
                                max = 0.00;
                                min = 20.00;
                        }
                }
        }

	public static void main(String[] args) throws Exception {
		Configuration conf6 = new Configuration();
                Job job6 = Job.getInstance(conf6, "Job6");
                job6.setJarByClass(clusterScores.class);
                job6.setMapperClass(clusterScores.Mapper3.class);
                job6.setReducerClass(clusterScores.Reducer3.class);
                job6.setNumReduceTasks(1);
                job6.setMapOutputKeyClass(Text.class);
                job6.setMapOutputValueClass(Text.class);
                job6.setOutputKeyClass(Text.class);
                job6.setOutputValueClass(NullWritable.class);
                job6.addCacheFile(new URI("/input/shortest-path-distance-matrix.txt"));
                FileInputFormat.addInputPath(job6, new Path(args[0]));
                FileOutputFormat.setOutputPath(job6, new Path(args[1]));
		job6.waitForCompletion(true);

		Configuration conf1 = new Configuration();
                Job job1 = Job.getInstance(conf1, "Job1");
                job1.setJarByClass(clusterScores.class);
                job1.setMapperClass(clusterScores.Mapper1.class);
                job1.setReducerClass(clusterScores.Reducer1.class);
                job1.setNumReduceTasks(1);
                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(NullWritable.class);
                job1.addCacheFile(new URI("/input/shortest-path-distance-matrix.txt"));
                FileInputFormat.addInputPath(job1, new Path(args[1]));
                FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
                Job job2 = Job.getInstance(conf2, "Job2");
                job2.setJarByClass(clusterScores.class);
                job2.setMapperClass(clusterScores.Mapper1.class);
                job2.setReducerClass(clusterScores.Reducer2.class);
                job2.setNumReduceTasks(1);
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(NullWritable.class);
                job2.addCacheFile(new URI("/test5.txt/part-r-00000"));
                FileInputFormat.addInputPath(job2, new Path(args[2]));
                FileOutputFormat.setOutputPath(job2, new Path(args[3]));
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
	  }
}
