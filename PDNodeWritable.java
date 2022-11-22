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
    private LongWritable distance = new LongWritable(Long.MAX_VALUE);
    private LongWritable prev = new LongWritable(0);
    private MapWritable adjList = new MapWritable();

    public BooleanWritable flag = new BooleanWritable(true);

    public void PDNodeWritable() throws IOException {
        this.distance = new LongWritable(Long.MAX_VALUE);
        this.prev = new LongWritable(0);
        this.adjList = new MapWritable();
        this.flag = new BooleanWritable(true);
    }

    public void set (LongWritable distance, LongWritable prev, MapWritable adjList, BooleanWritable flag){
        this.distance = distance;
        this.prev = prev;
        this.adjList = adjList;
        this.flag = flag;
    }

    public void setDistance(LongWritable distance){
        this.distance = distance;
    }

    public void setPrev(LongWritable prev){
        this.prev = prev;
    }

    public void setAdjList(MapWritable adjList){
        this.adjList = adjList;
    }

    public void setFlag(BooleanWritable flag){
        this.flag = flag;
    }


    public LongWritable getDistance() {
        return this.distance;
    }

    public MapWritable getAdjList() {
        return this.adjList;
    }

    public LongWritable getPrev(){
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
        LongWritable distance = this.distance;
        LongWritable prev = this.prev;
        MapWritable adjList = this.adjList;
        BooleanWritable flag = this.flag;
        String s = new String(" ");
        Set<Writable> keys = adjList.keySet();
        for (Writable key : keys) {
            LongWritable count = (LongWritable) adjList.get(key);
            s = s + key.toString() + ":" + count.toString() + "," ;
        }
        s = s + " ";
        result.append( distance.toString() + " " + prev.toString() + " " + flag.toString() + s );
        return result.toString();

    }

    public String toString(LongWritable nid) {
        StringBuilder result = new StringBuilder();
        LongWritable distance = this.distance;
        LongWritable prev = this.prev;
        MapWritable adjList = this.adjList;
        BooleanWritable flag = this.flag;
        String s = new String(" ");
        Set<Writable> keys = adjList.keySet();
        for (Writable key : keys) {
            LongWritable count = (LongWritable) adjList.get(key);
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
    	    

    public static Map<Long,Long> getStringToMap(String str){
	String[] str1 = str.split(",");
	//创建Map对象
	Map<Long,Long> map = new HashMap<>();
	//循环加入map集合
	for (int i = 0; i < str1.length; i++) {
		//根据":"截取字符串数组
		String[] str2 = str1[i].split(":");
		//str2[0]为KEY,str2[1]为值
		long int1 = Long.parseLong(str2[0]);
		long int2 = Long.parseLong(str2[1]);
		map.put(int1,int2);
        }
        return map;
    }


    public Long getByText(Text t){
	PDNodeWritable node = new PDNodeWritable();
	String str = t.toString();
	String[] all = str.trim().split(" ");
	String[] nodeAndDist = all[0].split("\t");
	long nid = Long.parseLong(nodeAndDist[0]);
	Long distance = Long.parseLong(nodeAndDist[1]);
	LongWritable distanceWritable = new LongWritable(distance);

	long prev = Long.parseLong(all[1]);
        LongWritable prevWritable = new LongWritable(prev);

	boolean flag = Boolean.parseBoolean(all[2]);
        BooleanWritable flagWritable = new BooleanWritable(flag);
    MapWritable mapWritable = new MapWritable();
    if(all.length == 4)
    {
        Map<Long,Long> map = getStringToMap(all[3]);


        for (Map.Entry<Long, Long> entry : map.entrySet()) {
            long key = entry.getKey();
            long value = entry.getValue();
            LongWritable keyWritable = new LongWritable(key);
            LongWritable valueWritable = new LongWritable(value);
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
