import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PDPreProcess {

    public static class PDPreProMapper
            extends Mapper<Object, Text, IntWritable, MapWritable>{

        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            IntWritable point = new IntWritable();
            MapWritable edgeMap = new MapWritable();
	    StringTokenizer itr = new StringTokenizer(value.toString());
            int u = Integer.valueOf(itr.nextToken());
            int v = Integer.valueOf(itr.nextToken());
            int e = Integer.valueOf(itr.nextToken());
            point.set(u);
            IntWritable vIntW = new IntWritable();
            vIntW.set(v);
            IntWritable eIntW = new IntWritable();
            eIntW.set(e);
            edgeMap.put(vIntW, eIntW);
            context.write(point, edgeMap);
        }
    }

    public static class PDPreProReducer
            extends Reducer<IntWritable,MapWritable,IntWritable,PDNodeWritable> {
        private IntWritable point = new IntWritable();
      
        public void reduce(IntWritable key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            MapWritable adjMap = new MapWritable();
	    PDNodeWritable node = new PDNodeWritable();
	    BooleanWritable flag = new BooleanWritable(true);
	    Configuration conf = context.getConfiguration();
            int src = Integer.parseInt(conf.get("src"));
            IntWritable srcIntWri = new IntWritable(src);
            for (MapWritable edgeMap : values) {
                for (Writable keywritable: edgeMap.keySet()){
                    IntWritable keyIntWritable = (IntWritable) keywritable;
                    IntWritable edgeLen = new IntWritable();
                    edgeLen = (IntWritable) edgeMap.get(keyIntWritable);
                    adjMap.put(keyIntWritable, edgeLen);
                }
            }
            if(key.get() == src)
            {
                IntWritable distance = new IntWritable(0);
                node.set(distance, srcIntWri, adjMap, flag);
            }
            else
            {
                IntWritable distance = new IntWritable(Integer.MAX_VALUE);
                node.set(distance, srcIntWri, adjMap, flag);
            }
            context.write(key, node);
        }
    }
/*
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("src", args[2]);
        Job job = Job.getInstance(conf, "PDPreProcess");
        job.setJarByClass(PDPreProcess.class);
        job.setMapperClass(PDPreProcess.PDPreProMapper.class);
//        job.setCombinerClass(PDPreProcess.PDPreProReducer.class);
        job.setReducerClass(PDPreProcess.PDPreProReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    }
*/    
}

