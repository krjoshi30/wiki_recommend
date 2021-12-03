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
import java.lang.*;

public class Category {
    public static class MapCategories extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String Tokens[] = value.toString().split("\\s+");
            String Subjects[] = Tokens[1].split("\\.");
            String c_string = "";
            for (int i = 1; i < Subjects.length-1; i++) {
                c_string = c_string.concat(Subjects[i] + ",");
            }
            c_string = c_string.concat(Subjects[Subjects.length-1]);
            context.write(new Text(Tokens[0]), new Text(c_string));
        }
    }
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String out = "";
            for (Text v : values) {
                out = out.concat(v.toString()+",");
            }
            
            if (!out.equals("")) {
                out = out.substring(0, out.length()-1);
            }
            String categories = "";
            ArrayList<String> outWords = new ArrayList<String>(Arrays.asList(out.split(",")));
            for (int i = 0; i < outWords.size(); i++) {
                if (!(categories.contains(","+outWords.get(i)) || categories.contains(outWords.get(i)+","))) {
                    categories = categories.concat(outWords.get(i)+",");
                }
            }
            
            if (!categories.equals("")) {
                categories = categories.substring(0, categories.length()-1);
            }
            context.write(key, new Text(categories));
        }
    }
    public static class OutReducer extends Reducer<Text, Text, Text, Text> {
    
        private ArrayList<Map.Entry<String, String>> NetNodes;
        private TreeMap<String, String> nodeMap;
        private TreeMap<String, String> interCategory;
    
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodeMap = new TreeMap<String, String>();
            NetNodes = new ArrayList<Map.Entry<String, String>>();
            interCategory = new TreeMap<String, String>();
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                nodeMap.put(key.toString(), v.toString());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            TreeMap<String, String> temp = new TreeMap<String, String>(nodeMap);
            for (Map.Entry<String, String> m1 : nodeMap.entrySet()) {
                for (Map.Entry<String, String> m2 : temp.entrySet()) {
                    if (!m1.equals(m2)) {
                        int sim = 0;
                        String m1Cat[] = m1.getValue().split(",");
                        String m2Cat = m2.getValue();
                        for (int i = 0; i < m1Cat.length; i++) {
                            if (m2Cat.contains(","+m1Cat[i]) || m2Cat.contains(m1Cat[i]+",")) {
                                sim++;
                            }
                        }
                        interCategory.put((m1.getKey()+" "+m2.getKey()), Integer.toString(sim));
                    }
                }
                temp.remove(m1.getKey());
            }
            
            for (Map.Entry<String, String> m : interCategory.entrySet()) {
                context.write(new Text(m.getKey()), new Text(m.getValue()));
            }
            
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration extract = new Configuration();
        Job jobExtract = Job.getInstance(extract, "extract categroies new");
        jobExtract.setJarByClass(Category.class);

        // Set up Mapper and Reducer
        jobExtract.setMapperClass(Category.MapCategories.class);
        jobExtract.setCombinerClass(Category.Combiner.class);
        jobExtract.setReducerClass(Category.OutReducer.class);
        jobExtract.setNumReduceTasks(1);

        //
        jobExtract.setMapOutputKeyClass(Text.class);
        jobExtract.setMapOutputValueClass(Text.class);
        jobExtract.setOutputKeyClass(Text.class);
        jobExtract.setOutputValueClass(Text.class);

        // IO
        FileInputFormat.addInputPath(jobExtract, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobExtract, new Path(args[1]));

        // Exit command
        System.exit(jobExtract.waitForCompletion(true) ? 0 : 1);
    }
}

