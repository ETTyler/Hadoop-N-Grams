import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Ngram {

    public static class NGMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        List words = new ArrayList();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z ]", "").toLowerCase());

            while (itr.hasMoreTokens()) {
                words.add(itr.nextToken());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sets what size 'n' grams to use based off the argument set
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            StringBuffer str = new StringBuffer("");
            for (int i = 0; i < words.size() - n; i++) {
                int gram = i;
                for(int j = 0; j < n; j++) {
                    if(j > 0) {
                        str = str.append(" ");
                        str = str.append(words.get(gram));
                    } else
                    {
                        str = str.append(words.get(gram));
                    }
                    gram++;
                }
                word.set("{"+str.toString()+",");
                str=new StringBuffer("");
                context.write(word, one);
            }
        }

    }

    public static class NGReducer
            extends Reducer<Text,IntWritable,Text,Text> {
        private IntWritable result = new IntWritable();
        private Text res = new Text();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            res.set(result+"},");
            context.write(key, res);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("n", args[2]);
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "Ngrams");
        job.setJarByClass(Ngram.class);
        job.setMapperClass(NGMapper.class);
        job.setReducerClass(NGReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}