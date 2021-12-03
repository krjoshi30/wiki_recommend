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

public class Kmeans {

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
                private Set<String> tiedClusters;
		private int numOfPages = 6000;
		private TreeMap<String,String> shortestPaths;


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
                	tiedClusters = new HashSet<String>();
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
                	numOfPages = s.length();
		}

                @Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
			String page = "";
                        String s = "";
			int temp = numOfPages + 5000;
                        char underscore = '_';
			char zero = '0';
			int test = 0;
			String test1 = "";
			Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries2 = shortestPaths.entrySet();
			for(int i = 0; i < numOfPages; i++) {
                                for(Map.Entry<String,String> entry : entries) {
					char c = (entry.getValue()).charAt(i);
					for(Map.Entry<String,String> entry2 : entries2) {
						if(entry.getKey().equals(entry2.getKey())) {
							test1 = entry2.getValue();
							break;
						}
					}
					char k = test1.charAt(Integer.parseInt(entry.getKey())-1);

					if(c != zero) {
						int x = 0;
						int y = 0;
						if(k == underscore) {
							y = 4;
						}
						if(c == underscore) {
							x = 4;
						}
						if(x == 0) {
							x = Character.getNumericValue(c);
						}
						if(y == 0) {
							y = Character.getNumericValue(k);
						}
						int z = x + y;
						if(z < temp) {
                                                        temp = z;
                                                        s = entry.getKey();
                                                        page = Integer.toString(i + 1);
                                                        test = 1;
                                                	tiedClusters.clear();
						}
                                                else if(z == temp) {
                                                        temp = z;
                                                        s = entry.getKey();
                                                        page = Integer.toString(i + 1);
                                                        test = 0;
                                                        tiedClusters.add(s);
                                                }

					}
                                }

                                if(temp < (numOfPages + 4000)) {
					if(test == 1) {
						String s1 = page + " " + s;
        	                                context.write(new Text(s1),NullWritable.get());
                                	}
					else {
						context.write(new Text(page + " 5000"),NullWritable.get());
					}
				}
                                temp = numOfPages + 5000;
                        }
                }
	}

	public static class Reducer8 extends Reducer<Text, Text, Text, NullWritable> {
                private TreeMap<String,String> treeMap;
                private Set<String> centralCluster;
		private TreeMap<String,String> shortestPaths;
		private List<String> list;
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
                        list = new ArrayList<String>();
                	list2 = new ArrayList<String>();
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
                        String temp = ",";
			int count = 0;
			int count2 = 0;
			String s1 = "";
			String path = "4";
			String path2 = "";
			int total = 0;
			char c = '1';
			char underscore = '_';
			double fin = 0.00;
			double fin2 = 500.00;
			String page = "";
			String cluster = "";
			Set<Map.Entry<String,String>> entries2 = treeMap.entrySet();
			Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries3 = shortestPaths.entrySet();
			Set<Map.Entry<String,String>> entries4 = shortestPaths.entrySet();

			for(Map.Entry<String,String> entry : entries) {
				if(entry.getKey().equals("5000")) {
					temp = entry.getValue();
				}

			}

			for(int i = 0; i < temp.length(); i++) {
                                if(temp.charAt(i) == ',') {
                                        count++;
                                }
                        }
                        for(int j = 0; j < count; j++) {
                                s1 = temp.substring(0, temp.indexOf(","));
                                list.add(s1);
                                temp = temp.substring(temp.indexOf(",") + 1,temp.length());
                        }
                        list.add(temp);

			for(int i = 0; i < list.size(); i++) {
				for(Map.Entry<String,String> entry3 : entries3) {
                                	if(entry3.getKey().equals(list.get(i))) {
                                        	path = entry3.getValue();
						break;
                                     	}
                             	}

				for(Map.Entry<String,String> entry2 : entries2) {
                                       	char d = '1';
                                       	total = 0;
                                       	count = 0;
                                       	count2 = 0;
                                      	if(entry2.getKey().equals("5000")) {}
					else {
						temp = entry2.getValue();
                                              	if(temp.contains(",")) {
                                                	for(int m = 0; m < temp.length(); m++) {
                                                            	if(temp.charAt(m) == ',') {
                                                                     	count++;
                                                            	}
                                                     	}
                                                      	for(int j = 0; j < count; j++) {
                                                      		s1 = temp.substring(0, temp.indexOf(","));
                                                              	list2.add(s1);
                                                              	temp = temp.substring(temp.indexOf(",") + 1,temp.length());
                                                     	}
                                                      	list2.add(temp);
							list2.add(entry2.getKey());

							for(int k = 0; k < list2.size(); k++) {
                                                       		c = path.charAt(Integer.parseInt(list2.get(k)) - 1);
								int x = 0;
                                                               	//int y = 0;
                                                               	if(c == underscore) {
                                                                   	x = 4;
                                                               	}
								//if(d == underscore) {
                                                                //        y = 4;
                                                                //}
                                                             	if(x == 0) {
                                                                   	x = Character.getNumericValue(c);
                                                              	}
                                                               	//if(y == 0) {
                                                                //   	y = Character.getNumericValue(d);
                                                              	//}
                                                              	total = total + x;
                                                               	count2++;
							}
                                                      	fin = (1.00*total)/(1.00*count2);
							//context.write(new Text(entry2.getKey() + " " + list.get(i) + " " + fin), NullWritable.get());
							if(fin < fin2) {
                                                              	fin2 = fin;
                                                               	page = list.get(i);
                                                              	cluster = entry2.getKey();
                                                        }
						}
						else if(entry2.getValue().equals("none")) {
							c = path.charAt(Integer.parseInt(entry2.getKey()) - 1);
                                                        int x = 0;
                                                                //int y = 0;
                                                        if(c == underscore) {
                                                                x = 4;
                                                        }
                                                                //if(d == underscore) {
                                                                //        y = 4;
                                                                //}
                                                        if(x == 0) {
                                                                x = Character.getNumericValue(c);
                                                        }
                                                                //if(y == 0) {
                                                                //      y = Character.getNumericValue(d);
                                                                //}
                                                        total = total + x;
                                                        count2++;
                                                        fin = (1.00*total)/(1.00*count2);
                                                        //context.write(new Text(entry2.getKey() + " " + list.get(i) + " " + fin), NullWritable.get());
                                                        if(fin < fin2) {
                                                                page = list.get(i);
                                                                cluster = entry2.getKey();
                                                      	}
                                                }
						else {
							c = path.charAt(Integer.parseInt(entry2.getKey()) - 1);
							int x = 0;
                                                                //int y = 0;
                                                      	if(c == underscore) {
                                                		x = 4;
                                                      	}
                                                                //if(d == underscore) {
                                                                //        y = 4;
                                                                //}
                                                     	if(x == 0) {
                                                          	x = Character.getNumericValue(c);
                                                      	}
                                                                //if(y == 0) {
                                                                //      y = Character.getNumericValue(d);
                                                                //}
                                                       	total = total + x;
                                                      	count2++;

							c = path.charAt(Integer.parseInt(entry2.getValue()) - 1);
                                                        x = 0;
                                                                //int y = 0;
                                                        if(c == underscore) {
                                                                x = 4;
                                                        }
                                                                //if(d == underscore) {
                                                                //        y = 4;
                                                                //}
                                                        if(x == 0) {
                                                                x = Character.getNumericValue(c);
                                                        }
                                                                //if(y == 0) {
                                                                //      y = Character.getNumericValue(d);
                                                                //}
                                                        total = total + x;
                                                        count2++;

                                              		fin = (1.00*total)/(1.00*count2);
                                            		//context.write(new Text(entry2.getKey() + " " + list.get(i) + " " + fin), NullWritable.get());
                                             		if(fin < fin2) {
								page = list.get(i);
                                                		cluster = entry2.getKey();
                                              		}
						}
					}
					list2.clear();
                                	fin = 0.00;
                                	count2 = 0;
                                	total = 0;
				}
				list2.clear();
				context.write(new Text(cluster + " " + page), NullWritable.get());
				fin2 = 500.00;
			}
		}

	}

	public static class Mapper6 extends Mapper<Object, Text, Text, Text> {
		private Text node1;
                private Text node2;
		private Set<String> centralCluster;

		@Override
                protected void setup(Context context) throws IOException, InterruptedException {
                        centralCluster = new HashSet<String>();

                        URI[] cacheFiles = context.getCacheFiles();

                        if (cacheFiles != null && cacheFiles.length > 0) {
                                try {
                                        String line = "";
                                        FileSystem fs = FileSystem.get(context.getConfiguration());
                                        Path getFilePath = new Path(cacheFiles[0].toString());
                                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                                        while ((line = reader.readLine()) != null) {
                                                String[] words = line.split(" ");
                                                        centralCluster.add(words[0]);
                                        }
                                }
                                catch (Exception e) {
                                        System.out.println("Unable to read the File");
                                        System.exit(1);
                                }
                        }
                }

		@Override
                protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
                        int j = 0;
                        while (itr.hasMoreTokens()) {
                                String s1 = itr.nextToken();
                                String s2 = itr.nextToken();
                                for(String m : centralCluster) {
                                        if(s1.equals(m)) {
                                                j = 1;
                                        }
                                }
                                if(j == 0) {
                                        node1 = new Text(s2);
                                        node2 = new Text(s1);
                                        context.write(node1, node2);
                                        j = 0;
                                }
                        }
                }
	}

	public static class Reducer6 extends Reducer<Text, Text, Text, NullWritable> {
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
                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        for(Map.Entry<String,String> entry : entries) {
                                String s = entry.getKey() + " " + entry.getValue();
                                context.write(new Text(s), NullWritable.get());
                        }
                }
        }

	public static class Reducer7 extends Reducer<Text, Text, Text, NullWritable> {
		private TreeMap<String,String> treeMap;
		private Set<String> centralCluster;

                @Override
                protected void setup(Context context) throws IOException, InterruptedException {
                        centralCluster = new HashSet<String>();

                        URI[] cacheFiles = context.getCacheFiles();

                        if (cacheFiles != null && cacheFiles.length > 0) {
                                try {
                                     	String line = "";
                                        FileSystem fs = FileSystem.get(context.getConfiguration());
                                        Path getFilePath = new Path(cacheFiles[0].toString());
                                        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                                        while ((line = reader.readLine()) != null) {
                                           	centralCluster.add(line);
                                        }
                                }
                                catch (Exception e) {
                                        System.out.println("Unable to read the File");
                                        System.exit(1);
                                }
                        }
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
                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
			int test = 1;
			for(String m : centralCluster) {
				for(Map.Entry<String,String> entry : entries) {
					if(entry.getKey().equals(m)) {
						test = 0;
					}
				}
				if(test == 1) {
					treeMap.put(m,"none");
				}
				test = 1;
			}

			Set<Map.Entry<String,String>> entries2 = treeMap.entrySet();
                        for(Map.Entry<String,String> entry2 : entries2) {
                                String s = entry2.getKey() + " " + entry2.getValue();
                                context.write(new Text(s), NullWritable.get());
                        }
                }
	}

	public static class Mapper2 extends Mapper<Object, Text, Text, Text> {
                private Text node1;
		private Text node2;
		private Set<String> centralCluster;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			centralCluster = new HashSet<String>();

			URI[] cacheFiles = context.getCacheFiles();

			if (cacheFiles != null && cacheFiles.length > 0) {
            			try {
					String line = "";
					FileSystem fs = FileSystem.get(context.getConfiguration());
			                Path getFilePath = new Path(cacheFiles[0].toString());
					BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

					while ((line = reader.readLine()) != null) {
                                                String[] words = line.split(" ");
                                                        centralCluster.add(words[0]);
                                        }
            			}
				catch (Exception e) {
                			System.out.println("Unable to read the File");
                			System.exit(1);
            			}
			}
		}

                @Override
                protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
			int j = 0;
                        while (itr.hasMoreTokens()) {
                                String s1 = itr.nextToken();
				String s2 = itr.nextToken();
                                for(String m : centralCluster) {
					if(s2.equals(m)) {
						j = 1;
					}
				}
				if(j == 0) {
					node1 = new Text(s1);
					node2 = new Text(s2);
                                	context.write(node1, node2);
					j = 0;
                        	}
			}
                }
        }

	public static class Reducer10 extends Reducer<Text, Text, Text, NullWritable> {
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
			Set<Map.Entry<String,String>> entries2 = treeMap.entrySet();
			for(Map.Entry<String,String> entry : entries2) {
				String s = entry.getKey();
				if(s.equals("5000")) {}
				else {
                                	context.write(new Text(entry.getKey() + " " + entry.getValue()), NullWritable.get());
                        	}
			}
                }
        }

	public static class Reducer2 extends Reducer<Text, Text, Text, NullWritable> {
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
                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        for(Map.Entry<String,String> entry : entries) {
                                String s = entry.getKey() + "," + entry.getValue();
                                context.write(new Text(s), NullWritable.get());
                        }
                }
        }

	public static class Mapper3 extends Mapper<Object, Text, Text, NullWritable> {

                private Text node1;

                @Override
                protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
                        	String s = itr.nextToken();
                        	node1 = new Text(s);
                        	context.write(node1,NullWritable.get());
			}
                }
        }


	public static class Reducer3 extends Reducer<Text, NullWritable, Text, NullWritable> {

                private Set<String> treeMap;
		private List<String> list;
		private TreeMap<String,String> shortestPaths;

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
			treeMap = new HashSet<String>();
                	list = new ArrayList<String>();
		}

                @Override
                protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
                        treeMap.add(key.toString());
                }

		@Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
			int count = 0;
			int count2 = 0;
			double sum = 50000.000;
			String page = "";
			String s = "";
			char underscore = '_';
                        char zero = '0';
			double tempSum = 0.000;
			String s1 = "";
			String s3 = "";
			Set<Map.Entry<String,String>> entries2 = shortestPaths.entrySet();
			for(String w : treeMap) {
				for(int e = 0; e < w.length(); e++) {
					if(w.charAt(e) == ',') {
						count++;
					}
				}
				for(int k = 0; k < count; k++) {
					s1 = w.substring(0, w.indexOf(","));
					list.add(s1);
					w = w.substring(w.indexOf(",") + 1,w.length());
				}
				list.add(w);

				for(int i = 0; i < list.size(); i++) {
					for(Map.Entry<String,String> entry2 : entries2) {
						if(entry2.getKey().equals(list.get(i))) {
							s = entry2.getValue();
							break;
                                                }
                                        }

					for(int j = 0; j < list.size(); j++) {
						int x = Integer.parseInt(list.get(j)) - 1;
						char c = s.charAt(x);
						if(c == zero) {
                                                        tempSum = tempSum;
                                                }
						else {
							for(Map.Entry<String,String> entry2 : entries2) {
                                                 		if(entry2.getKey().equals(list.get(j))) {
                                                                	s3 = entry2.getValue();
                                                                	break;
                                                        	}
                                                	}
							int y = Integer.parseInt(list.get(i)) - 1;
							char k = s3.charAt(y);
							if(c == underscore) {
                                                        	tempSum = tempSum + 4;
                                                        	count2++;
                                                	}
							else {
								tempSum = tempSum + Character.getNumericValue(c);
                                                        	count2++;
							}
							if(k == underscore) {
								tempSum = tempSum + 4;
                                                                count2++;
							}
							else {
                                                                tempSum = tempSum + Character.getNumericValue(k);
                                                                count2++;
                                                        }
						}
					}

					double num1 = (1.000*tempSum)/(1.000*count2);
					double num2 = (1.000*sum)/(1.000*count2);

					if(num1 < num2) {
						sum = tempSum;
						page = list.get(i);
					}
					tempSum = 0.000;
					count2 = 0;
				}
                                context.write(new Text(page), NullWritable.get());
				s1 = "";
				sum = 50000.000;
				tempSum = 0.000;
				count = 0;
				count2 = 0;
				list.clear();
                        }
                }
        }

	public static class Reducer4 extends Reducer<Text, NullWritable, Text, NullWritable> {

                private Set<String> treeMap;
                private TreeMap<String,String> shortestPaths;

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
                        treeMap = new HashSet<String>();
                }

                @Override
                protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
                        treeMap.add(key.toString());
                }

                @Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<Map.Entry<String,String>> entries = shortestPaths.entrySet();
                        for(String w : treeMap) {
				for(Map.Entry<String,String> entry : entries) {
                               		if(entry.getKey().equals(w)) {
                                        	context.write(new Text(w + " " + entry.getValue()), NullWritable.get());
						break;
					}
				}
			}
		}
	}

	public static class Reducer11 extends Reducer<Text, Text, Text, NullWritable> {

                private TreeMap<String, String> treeMap;
                private TreeMap<String, String> shortestPaths;
                private List<String> list1;

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
                        String temp = "";
                        double averageLength = 0.00;
                        double total = 500.00;
                        String node = "";
                        int sum = 0;
                        int count = 0;

                        Set<Map.Entry<String,String>> entries = treeMap.entrySet();
                        Set<Map.Entry<String,String>> entries2 = shortestPaths.entrySet();
                        Set<Map.Entry<String,String>> entries3 = shortestPaths.entrySet();

                        for(Map.Entry<String,String> entry : entries) {
				count = 0;
                                s = entry.getValue();
                                if(s.contains(",")) {
                                        for(int j = 0; j < s.length(); j++) {
                                                if(s.charAt(j) == ',') {
                                                        count++;
                                                }
                                        }
                                        for(int k = 0; k < count; k++) {
                                                if(s.contains(",")) {
                                                        s2 = s.substring(0,s.indexOf(","));
                                                        if(s2.equals("none")){}
                                                        else {
                                                              	list1.add(s2);
                                                        }
                                                        s = s.substring(s.indexOf(",") + 1,s.length());
                                                }
                                        }
                                        if(s.equals("none")){}
                                        else {
                                              	list1.add(s);
                                        }
                                        list1.add(entry.getKey());
                                }
				else {
                                        if(s.equals("none")){}
                                        else {
                                                list1.add(s);
                                        }
                                        list1.add(entry.getKey());
                                }

                                if(list1.size() < 2) {
                                        context.write(new Text(list1.get(0)),NullWritable.get());
                                        total = 500.00;
                                        list1.clear();
                                }

                                else {
					for (int l = 0; l < list1.size(); l++) {
                                                temp = "";
                                                sum = 0;
                                                for(Map.Entry<String,String> entry2 : entries2) {
                                                        if(entry2.getKey().equals(list1.get(l))) {
                                                                s3 = entry2.getValue();
                                                                break;
                                                        }
                                                }

                                                for (int m = 0; m < list1.size(); m++) {
                                                        if(l != m) {
                                                                for(Map.Entry<String,String> entry3 : entries3) {
                                                                        if(entry3.getKey().equals(list1.get(m))) {
                                                                                s4 = entry3.getValue();
                                                                                break;
                                                                        }
                                                                }
                                                                int x = Integer.parseInt(list1.get(m)) - 1;
                                                                int y = Integer.parseInt(list1.get(l)) - 1;
                                                                char c = s3.charAt(x);
                                                                char d = s4.charAt(y);
                                                                temp = temp + c + d;
                                                        }
                                                }
						for(int v = 0; v < temp.length(); v++) {
                                                        if(temp.charAt(v) == '_') {
                                                                sum = sum + 5;
                                                        }
                                                        else {
                                                              	int q = temp.charAt(v) - '0';
                                                                sum = sum + q;
                                                        }
                                                }
                                                int z = temp.length();
                                                averageLength = (1.00*sum)/(1.00*z);
                                                if(averageLength < total) {
                                                        total = averageLength;
                                                        node = list1.get(l);
                                                }
                                        }
                                        list1.clear();
                                        context.write(new Text(node),NullWritable.get());
                                        total = 500.00;
                                }
                        }
                }
        }

	public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "Job");
                job.setJarByClass(Kmeans.class);
                job.setMapperClass(Kmeans.Mapper3.class);
                job.setReducerClass(Kmeans.Reducer4.class);
                job.setNumReduceTasks(1);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);
		job.addCacheFile(new URI("/input/shortest-path-distance-matrix.txt"));
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);

		Configuration conf1 = new Configuration();
                Job job1 = Job.getInstance(conf1, "Job1");
                job1.setJarByClass(Kmeans.class);
                job1.setMapperClass(Kmeans.Mapper1.class);
                job1.setReducerClass(Kmeans.Reducer1.class);
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
                job2.setJarByClass(Kmeans.class);
                job2.setMapperClass(Kmeans.Mapper6.class);
                job2.setReducerClass(Kmeans.Reducer6.class);
                job2.setNumReduceTasks(1);
                job2.setMapOutputKeyClass(Text.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(NullWritable.class);
                job2.addCacheFile(new URI("/clusters2.txt/part-r-00000"));
                FileInputFormat.addInputPath(job2, new Path(args[2]));
                FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);

		Configuration conf3 = new Configuration();
                Job job3 = Job.getInstance(conf3, "Job3");
                job3.setJarByClass(Kmeans.class);
                job3.setMapperClass(Kmeans.Mapper1.class);
                job3.setReducerClass(Kmeans.Reducer7.class);
                job3.setNumReduceTasks(1);
                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(Text.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(NullWritable.class);
                job3.addCacheFile(new URI("/input/initialClusters.txt"));
                FileInputFormat.addInputPath(job3, new Path(args[3]));
                FileOutputFormat.setOutputPath(job3, new Path(args[4]));
		job3.waitForCompletion(true);

		Configuration conf4 = new Configuration();
                Job job4 = Job.getInstance(conf4, "Job4");
                job4.setJarByClass(Kmeans.class);
                job4.setMapperClass(Kmeans.Mapper1.class);
                job4.setReducerClass(Kmeans.Reducer8.class);
                job4.setNumReduceTasks(1);
                job4.setMapOutputKeyClass(Text.class);
                job4.setMapOutputValueClass(Text.class);
                job4.setOutputKeyClass(Text.class);
                job4.setOutputValueClass(NullWritable.class);
                job4.addCacheFile(new URI("/input/shortest-path-distance-matrix.txt"));
                FileInputFormat.addInputPath(job4, new Path(args[4]));
                FileOutputFormat.setOutputPath(job4, new Path(args[5]));
		job4.waitForCompletion(true);

		Configuration conf5 = new Configuration();
                Job job5 = Job.getInstance(conf5, "Job5");
                job5.setJarByClass(Kmeans.class);
                job5.setReducerClass(Kmeans.Reducer10.class);
                job5.setNumReduceTasks(1);
                job5.setMapOutputKeyClass(Text.class);
                job5.setMapOutputValueClass(Text.class);
                job5.setOutputKeyClass(Text.class);
                job5.setOutputValueClass(NullWritable.class);
		MultipleInputs.addInputPath(job5,new Path(args[4]), TextInputFormat.class, Mapper1.class);
                MultipleInputs.addInputPath(job5,new Path(args[5]), TextInputFormat.class, Mapper1.class);
		FileOutputFormat.setOutputPath(job5, new Path(args[6]));
		job5.waitForCompletion(true);

		Configuration conf6 = new Configuration();
                Job job6 = Job.getInstance(conf6, "Job6");
                job6.setJarByClass(Kmeans.class);
                job6.setMapperClass(Kmeans.Mapper1.class);
                job6.setReducerClass(Kmeans.Reducer11.class);
                job6.setNumReduceTasks(1);
                job6.setMapOutputKeyClass(Text.class);
                job6.setMapOutputValueClass(Text.class);
                job6.setOutputKeyClass(Text.class);
                job6.setOutputValueClass(NullWritable.class);
                job6.addCacheFile(new URI("/input/shortest-path-distance-matrix.txt"));
                FileInputFormat.addInputPath(job6, new Path(args[6]));
                FileOutputFormat.setOutputPath(job6, new Path(args[7]));
                System.exit(job6.waitForCompletion(true) ? 0 : 1);

	  }
}
