import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
            extends Mapper<Object, Text, LongWritable, MapWritable>{

        private final static LongWritable one = new LongWritable(1);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            LongWritable point = new LongWritable();
            MapWritable edgeMap = new MapWritable();
	    StringTokenizer itr = new StringTokenizer(value.toString());
            long u = Long.valueOf(itr.nextToken());
            long v = Long.valueOf(itr.nextToken());
            long e = Long.valueOf(itr.nextToken());
            point.set(u);
            LongWritable vLongW = new LongWritable();
            vLongW.set(v);
            LongWritable eLongW = new LongWritable();
            eLongW.set(e);
            edgeMap.put(vLongW, eLongW);
            context.write(point, edgeMap);
        }
    }

    public static class PDPreProReducer
            extends Reducer<LongWritable,MapWritable,LongWritable,PDNodeWritable> {
        private LongWritable point = new LongWritable();
      
        public void reduce(LongWritable key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            MapWritable adjMap = new MapWritable();
	    PDNodeWritable node = new PDNodeWritable();
	    BooleanWritable flag = new BooleanWritable(true);
	    Configuration conf = context.getConfiguration();
            long src = Long.parseLong(conf.get("src"));
            LongWritable srcLongWri = new LongWritable(src);
            for (MapWritable edgeMap : values) {
                for (Writable keywritable: edgeMap.keySet()){
                    LongWritable keyLongWritable = (LongWritable) keywritable;
                    LongWritable edgeLen = new LongWritable();
                    edgeLen = (LongWritable) edgeMap.get(keyLongWritable);
                    adjMap.put(keyLongWritable, edgeLen);
                }
            }
            if(key.get() == src)
            {
                LongWritable distance = new LongWritable(0);
                node.set(distance, srcLongWri, adjMap, flag);
            }
            else
            {
                LongWritable distance = new LongWritable(Long.MAX_VALUE);
                node.set(distance, srcLongWri, adjMap, flag);
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
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PDNodeWritable.class);
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    
    }
*/    
}

