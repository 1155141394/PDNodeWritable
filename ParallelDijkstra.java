import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Job;


public class ParallelDijkstra {
    public static class ParallelMapper
            extends Mapper<LongWritable, Text, LongWritable,PDNodeWritable>{

        public void map(LongWritable key, Text t, Context context
        ) throws IOException, InterruptedException {
	    PDNodeWritable value = new PDNodeWritable();
    	    long nid = (long)value.getByText(t);	   
	    LongWritable nidWritable = new LongWritable(nid); 
            IntWritable d = value.getDistance();


            context.write(nidWritable, value);
            if(d.get() != Integer.MAX_VALUE){
                MapWritable adjList = value.getAdjList();

                Set<Writable> nodes = adjList.keySet();
                for (Writable node : nodes) {
                    // d1 上一个点的距离
                    int d1 = d.get();
		    IntWritable d2Writable = (IntWritable)adjList.get(node);
                    // d2 上一个点到node的距离
                    int d2 = d2Writable.get();
                    // sum node的距离
                    IntWritable sum = new IntWritable();
                    sum.set(d1+d2);
                    PDNodeWritable N = new PDNodeWritable();
                    BooleanWritable flag = new BooleanWritable(false);
		    IntWritable prevWritable = new IntWritable();
                    int prev = (int)nidWritable.get();
                    prevWritable.set(prev);


		    MapWritable map = new MapWritable();
                    N.set(sum, prevWritable, map, flag);
		    IntWritable tmpWritable = (IntWritable)node;
		    int tmp = tmpWritable.get();
		    LongWritable nodeWritable = new LongWritable((long)tmp);
                    context.write(nodeWritable, N);
		    
                }
            }
        }
    }

    public static class ParallelReducer
            extends Reducer<LongWritable,PDNodeWritable,LongWritable,PDNodeWritable> {

        public void reduce(LongWritable key, Iterable<PDNodeWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Set max distance
            int dMin = Integer.MAX_VALUE;
            IntWritable preID = new IntWritable();
            // Create a new PDNode to store the node info
            PDNodeWritable InfoNode = new PDNodeWritable();

            for (PDNodeWritable node : values) {
		// Judge whether the node is node or dist
                if(node.getFlag().get())
                {
                    InfoNode.copy(node, key);
                }
                if(node.getDistance().get() < dMin)
                {
                    dMin = node.getDistance().get();
                    preID = node.getPrev();
                }
            }

            IntWritable finalDist = new IntWritable(dMin);

            InfoNode.setDistance(finalDist);
            InfoNode.setPrev(preID);

            context.write(key, InfoNode);
        }
    }

    public static void main(String[] args) throws Exception {
	String itr = args[3];
        Configuration conf1 = new Configuration();
        //获取job对象
	conf1.set("src", args[2]);
        Job job1 = Job.getInstance(conf1, "PreProcess");
        //设置job方法入口的驱动类
      	job1.setJarByClass(PDPreProcess.class);
        //设置Mapper组件类
        job1.setMapperClass(PDPreProcess.PDPreProMapper.class);
        //设置mapper的输出key类型
        job1.setMapOutputKeyClass(IntWritable.class);
        //设置Mappper的输出value类型，注意Text的导包问题
        job1.setMapOutputValueClass(MapWritable.class);
        //设置reduce组件类
        job1.setReducerClass(PDPreProcess.PDPreProReducer.class);
        //设置reduce输出的key和value类型
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(PDNodeWritable.class);
        //设置输入路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        //设置输出结果路径，要求结果路径事先不能存在
        FileOutputFormat.setOutputPath(job1, new Path("/user/hadoop/tmp/Output0/"));


	ControlledJob cjob1 = new ControlledJob(conf1);

	cjob1.setJob(job1);
        JobControl jc = new JobControl("PreProcess");
        jc.addJob(cjob1);
   
	Thread jcThread = new Thread(jc);  
        jcThread.start();  
        while(true){  
		if(jc.allFinished()){  
		       System.out.println(jc.getSuccessfulJobList()); 
		       System.out.println(jc.getFailedJobList()); 
		       jc.stop();  
		       break; 
		}  
	}
	int i = 0;
	int iteration = Integer.parseInt(itr);
	while(i < iteration){
		Configuration conf2 = new Configuration();
        	Job job2 = Job.getInstance(conf2,"Parallel");

        	job2.setJarByClass(ParallelDijkstra.class);

        	job2.setMapperClass(ParallelMapper.class);
        	job2.setMapOutputKeyClass(LongWritable.class);
        	job2.setMapOutputValueClass(PDNodeWritable.class);

        	job2.setReducerClass(ParallelReducer.class);
        	//设置reduce输出的key和value类型
        	job2.setOutputKeyClass(LongWritable.class);
        	job2.setOutputValueClass(PDNodeWritable.class);

        	FileInputFormat.setInputPaths(job2,new Path("/user/hadoop/tmp/Output"+ i));
		i++;
		if(i == iteration){
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		}else{
			FileOutputFormat.setOutputPath(job2, new Path("/user/hadoop/tmp/Output" + i));
		}
        	ControlledJob cjob2 = new ControlledJob(conf2);

        	cjob2.setJob(job2);
        	jc = new JobControl("Parallel");
        	jc.addJob(cjob2);

        	jcThread = new Thread(jc);
        	jcThread.start();
        	while(true){
                	if(jc.allFinished()){
                       		System.out.println(jc.getSuccessfulJobList());
                       		System.out.println(jc.getFailedJobList());
                       		jc.stop();
                       		break;
                	}
        	}

	}



    }
}
