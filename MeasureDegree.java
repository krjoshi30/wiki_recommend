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

import java.io.IOException;
import java.util.*;

public class MeasureDegree {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
            @Override
            protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String Tokens[] = value.toString().split("\\s+");
            String nodeIn = Tokens[1];
            String nodeOut = Tokens[0];
            context.write(new Text(nodeOut), new Text("o "));
            context.write(new Text(nodeIn), new Text("i "));
        }
    }
    public static class TokenizerMapperOut extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String Tokens[] = value.toString().split("\\s+");
            String nodeOut = Tokens[0];
            context.write(new Text(nodeOut), new IntWritable(1));
        }
    }
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String out = "";
            for (Text v : values) {
                out = out.concat(v.toString());
            }
            context.write(key, new Text(out));
        }
    }
    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
    
        private ArrayList<Map.Entry<String, String>> NetNodes;
        private TreeMap<String, String> nodeMap;
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodeMap = new TreeMap<String, String>();
            NetNodes = new ArrayList<Map.Entry<String, String>>(); 
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double outDegree = new Double(0.0);
            Double inDegree = new Double(0.0);
            Double sum = new Double(0.0);
            for (Text v : values) {
                //context.write(key, v);
                String Tokens[] = v.toString().split(" ");
                for (int i = 0; i < Tokens.length; i++) {
                    if (Tokens[i].equals("i")) {
                        inDegree = inDegree+1.0;
                        sum = sum+1.0;
                    }
                    if (Tokens[i].equals("o")) {
                        outDegree = outDegree+1.0;
                        sum = sum+1.0;
                    }
                }
            }
            if (inDegree.equals(0.0)) {
                inDegree = 1.0;
            }
            Double ratioScore = sum*(outDegree/inDegree);
            
            if (nodeMap.containsKey(key.toString())) {
                String mess = "\nduplicate \t"+key.toString()+"\n";
                context.write(new Text(mess), new Text(nodeMap.get(key.toString())));
            }
            nodeMap.put(key.toString(), String.valueOf(ratioScore));
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> m : nodeMap.entrySet()) {
                NetNodes.add(m);
            }
            Collections.sort(NetNodes, new Comparator<Map.Entry<String, String>>() {
                @Override
                public int compare(Map.Entry<String, String> a, Map.Entry<String, String> b) {
                    if(a.getValue().compareTo(b.getValue()) != 0) {
                        return b.getValue().compareTo(a.getValue());
                    }
                    return a.getKey().compareTo(b.getKey());
                }
            });
            for (Map.Entry<String, String> m : NetNodes) {
                context.write(new Text(m.getKey()), new Text(m.getValue()));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        // Measure Degree
        
        Configuration measureDegIn = new Configuration();
        Job jobMeasureIn = Job.getInstance(measureDegIn, "measure degrees in new");
        jobMeasureIn.setJarByClass(MeasureDegree.class);

        // Set up Mapper and Reducer
        jobMeasureIn.setMapperClass(MeasureDegree.TokenizerMapper.class);
        jobMeasureIn.setReducerClass(MeasureDegree.CountReducer.class);
        jobMeasureIn.setCombinerClass(MeasureDegree.Combiner.class);
        jobMeasureIn.setNumReduceTasks(1);

        //
        jobMeasureIn.setMapOutputKeyClass(Text.class);
        jobMeasureIn.setMapOutputValueClass(Text.class);
        jobMeasureIn.setOutputKeyClass(Text.class);
        jobMeasureIn.setOutputValueClass(Text.class);

        // IO
        FileInputFormat.addInputPath(jobMeasureIn, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobMeasureIn, new Path(args[1]));
        // Wait to complete
        System.exit(jobMeasureIn.waitForCompletion(true) ? 0 : 1);
    }
}

