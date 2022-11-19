import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*; // 引入 ArrayList 类
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


@InterfaceAudience.Public
@InterfaceStability.Stable

public class PDNodeWritable implements Writable {
    // Some data
    private IntWritable distance = new IntWritable(Integer.MAX_VALUE);
    private IntWritable prev = new IntWritable(0);
    private MapWritable adjList = new MapWritable();

    public BooleanWritable flag = new BooleanWritable(true);

    public void PDNodeWritable() throws IOException {
        this.distance = new IntWritable(Integer.MAX_VALUE);
        this.prev = new IntWritable(0);
        this.adjList = new MapWritable();
        this.flag = new BooleanWritable(true);
    }

    public void set (IntWritable distance, IntWritable prev, MapWritable adjList, BooleanWritable flag){
        this.distance = distance;
        this.prev = prev;
        this.adjList = adjList;
        this.flag = flag;
    }

    public void setDistance(IntWritable distance){
        this.distance = distance;
    }

    public void setPrev(IntWritable prev){
        this.prev = prev;
    }

    public void setAdjList(MapWritable adjList){
        this.adjList = adjList;
    }

    public void setFlag(BooleanWritable flag){
        this.flag = flag;
    }


    public IntWritable getDistance() {
        return this.distance;
    }

    public MapWritable getAdjList() {
        return this.adjList;
    }

    public IntWritable getPrev(){
        return this.prev;
    }

    public BooleanWritable getFlag() {
        return this.flag;
    }

    public void readFields(DataInput in) throws IOException {
        distance.readFields(in);
        prev.readFields(in);
        adjList.readFields(in);
        flag.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        distance.write(out);
        prev.write(out);
        adjList.write(out);
        flag.write(out);
    }

    public static PDNodeWritable read(DataInput in) throws IOException {
	PDNodeWritable pd = new PDNodeWritable();
	pd.readFields(in);
	return pd;
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        IntWritable distance = this.distance;
        IntWritable prev = this.prev;
        MapWritable adjList = this.adjList;
        BooleanWritable flag = this.flag;
        String s = new String(" ");
        Set<Writable> keys = adjList.keySet();
        for (Writable key : keys) {
            IntWritable count = (IntWritable) adjList.get(key);
            s = s + key.toString() + ":" + count.toString() + "," ;
        }
        result.append( distance.toString() + " " + prev.toString() + " " + flag.toString() + s );
        return result.toString();

    }

    public String toString(LongWritable nid) {
        StringBuilder result = new StringBuilder();
        IntWritable distance = this.distance;
        IntWritable prev = this.prev;
        MapWritable adjList = this.adjList;
        BooleanWritable flag = this.flag;
        String s = new String(" ");
        Set<Writable> keys = adjList.keySet();
        for (Writable key : keys) {
            IntWritable count = (IntWritable) adjList.get(key);
            s = s + key.toString() + ":" + count.toString() + "," ;
        }
        s = s + " ";
        result.append( nid.toString() + "\t" + distance.toString() + " " + prev.toString() + " " + flag.toString() + s );
        return result.toString();

    }

    public void copy(PDNodeWritable pd, LongWritable nid){
	String pdStr = pd.toString(nid);
	Text pdText = new Text();
	pdText.set(pdStr);
	this.getByText(pdText);
	return;	
    }
    	    

    public static Map<Integer,Integer> getStringToMap(String str){
	String[] str1 = str.split(",");
	//创建Map对象
	Map<Integer,Integer> map = new HashMap<>();
	//循环加入map集合
	for (int i = 0; i < str1.length; i++) {
		//根据":"截取字符串数组
		String[] str2 = str1[i].split(":");
		//str2[0]为KEY,str2[1]为值
		int int1 = Integer.parseInt(str2[0]);
		int int2 = Integer.parseInt(str2[1]);
		map.put(int1,int2);
        }
        return map;
    }


    public int getByText(Text t){
	PDNodeWritable node = new PDNodeWritable();
	String str = t.toString();
	String[] all = str.split(" ");
	String[] nodeAndDist = all[0].split("\t");
	int nid = Integer.parseInt(nodeAndDist[0]);
	int distance = Integer.parseInt(nodeAndDist[1]);
	IntWritable distanceWritable = new IntWritable(distance);

	int prev = Integer.parseInt(all[1]);
        IntWritable prevWritable = new IntWritable(prev);

	boolean flag = Boolean.parseBoolean(all[2]);
        BooleanWritable flagWritable = new BooleanWritable(flag);
    MapWritable mapWritable = new MapWritable();
    if(all.length == 3)
    {
        Map<Integer,Integer> map = getStringToMap(all[3]);


        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            int key = entry.getKey();
            int value = entry.getValue();
            IntWritable keyWritable = new IntWritable(key);
            IntWritable valueWritable = new IntWritable(value);
            mapWritable.put(keyWritable, valueWritable);
        }
    }

	
	this.distance = distanceWritable;
	this.prev = prevWritable;
	this.adjList = mapWritable;
	this.flag = flagWritable;
        return nid;
   
    }


}
