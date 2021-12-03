import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.*;
import java.lang.*;

public class ArticleMap {
    public static class MapCategories extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String Tokens[] = value.toString().split("\t");
            context.write(new Text(Tokens[0]), new Text(Tokens[1]));
        }
    }
    public static class MapScore extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String Tokens[] = value.toString().split("\t");
            context.write(new Text(Tokens[0]), new Text(Tokens[1]));
        }
    }
    public static class OutReducer extends Reducer<Text, Text, Text, Text> {
        
        private TreeMap<String, String> categories;
        private TreeMap<String, String> dScore;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            categories = new TreeMap<String, String>();
            dScore = new TreeMap<String, String>();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                if (key.toString().contains(" ")) {
                    categories.put(key.toString(), v.toString());
                } else {
                    dScore.put(key.toString(), v.toString());
                }
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("article_1, article2, "), new Text("score"));
            for (Map.Entry<String, String> m : categories.entrySet()) {
                String articles[] = m.getKey().split(" ");
                double catSim = Double.parseDouble(m.getValue());
                if (dScore.containsKey(articles[1])) {
                    double article1Degree = Double.parseDouble(dScore.get(articles[1]));
                    context.write(new Text(articles[0]+", "+articles[1]+", "), new Text(String.valueOf(catSim*article1Degree)));
                } else {
                    context.write(new Text(articles[0]+", "+articles[1]+", "), new Text("0.0"));
                }
                if (dScore.containsKey(articles[0])) {
                    double article0Degree = Double.parseDouble(dScore.get(articles[0]));
                    context.write(new Text(articles[1]+", "+articles[0]+", "), new Text(String.valueOf(catSim*article0Degree)));
                    
                } else {
                    context.write(new Text(articles[1]+", "+articles[0]+", "), new Text("0.0"));
                }
                
            }
        }
    
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job jobMap = Job.getInstance(conf, "map articles new");
        jobMap.setJarByClass(ArticleMap.class);

        // Set up Mapper and Reducer
        
        jobMap.setReducerClass(ArticleMap.OutReducer.class);
        MultipleInputs.addInputPath(jobMap, new Path(args[0]),
                    TextInputFormat.class, MapCategories.class);

            MultipleInputs.addInputPath(jobMap, new Path(args[1]),
                    TextInputFormat.class, MapScore.class);
        //
        jobMap.setMapOutputKeyClass(Text.class);
        jobMap.setMapOutputValueClass(Text.class);
        jobMap.setOutputKeyClass(Text.class);
        jobMap.setOutputValueClass(Text.class);
        jobMap.setOutputFormatClass(TextOutputFormat.class);

        // IO
        //FileInputFormat.addInputPath(jobMap, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobMap, new Path(args[2]));

        // Exit command
        System.exit(jobMap.waitForCompletion(true) ? 0 : 1);
    }
}
