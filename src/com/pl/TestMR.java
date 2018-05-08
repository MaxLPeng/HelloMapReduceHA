package com.pl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TestMR {	
 
	final static String LOCAL_PATH			= "e:/tmp";
	final static String LOCAL_FILE			= "/HDFSHello.txt";	
	//final static String FS_DEFAULTFS		= "hdfs://NameNode01:9000";
	final static String FS_DEFAULTFS		= "hdfs://namecluster1";
	final static String YARN_RM_HOSTNAME	= "NameNode01";
	final static String DFS_PATH_ROOT		= "/test";
	final static String DFS_PATH_IN			= DFS_PATH_ROOT + "/in";
	final static String DFS_PATH_OUT		= DFS_PATH_ROOT + "/out";
	final static String DFS_FULL_PATH_IN	= FS_DEFAULTFS + DFS_PATH_ROOT + "/in";
	final static String DFS_FULL_PATH_OUT	= FS_DEFAULTFS + DFS_PATH_ROOT + "/out";
	final static String DFS_FILE_IN			= "/HDFSHello.data";  
	final static String DFS_JAR				= "HelloMR-HA.jar";
	
    /**
     * 测试
     * @param args
     * @throws Exception
     */ 
	public static void main(String[] args) throws Exception {
		//本地运算  ，数据,结果都在本地
		//LocalCalcLocalData();
		//本地运算，数据下载到本地，本地运算完上传
		//LocalCalcRemoteData();
		//数据,运算，结果都在远程 (本地必须先导出的Jar包)
		RemoteCalcRemoteData();
	} 
	
    /**
     * 本地运算  ，数据,结果都在本地 
     * @throws Exception
     */ 
    public static void LocalCalcLocalData() throws Exception {
    	//配置服务器
        Configuration conf = new Configuration();  
        //删除历史输出目录
        FileSystem fileSystem = FileSystem.get(conf); 
        boolean reuslt= fileSystem.exists(new Path(LOCAL_PATH + "/out"));
        if (reuslt) {
			reuslt = fileSystem.delete(new Path(LOCAL_PATH + "/out"), true);
			System.out.println(LOCAL_PATH + "/out" + "   delete..." + reuslt);
        }
        //计算，输出
        Job job = Job.getInstance(conf); 
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, LOCAL_PATH + LOCAL_FILE);
		FileOutputFormat.setOutputPath(job, new Path(LOCAL_PATH + "/out")); 
        job.waitForCompletion(true);
    } 
    
    /**
     * 本地运算，数据下载到本地，本地运算完上传
     * @throws Exception
     */ 
    public static void LocalCalcRemoteData() throws Exception {
    	//配置服务器 
    	Configuration confRemote = new Configuration();
    	confRemote.set("fs.defaultFS", FS_DEFAULTFS); 
    	//HDFS-HA
    	confRemote.set("dfs.nameservices", "namecluster1");
    	confRemote.set("dfs.ha.namenodes.namecluster1", "nn1,nn2");
    	confRemote.set("dfs.namenode.rpc-address.namecluster1.nn1", "NameNode01:9000");
    	confRemote.set("dfs.namenode.rpc-address.namecluster1.nn2", "NameNode02:9000");
    	confRemote.set("dfs.client.failover.proxy.provider.namecluster1"
    			,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

    	FileSystem fileSystem = FileSystem.get(confRemote);  
    	//删除历史输出目录
    	boolean reuslt= fileSystem.exists(new Path(DFS_PATH_OUT));
    	if (reuslt) {
	    	reuslt = fileSystem.delete(new Path(DFS_PATH_OUT), true);
			System.out.println(DFS_PATH_OUT + "   delete..." + reuslt); 
    	}
    	//计算，输出
        Configuration conf = new Configuration();
        //conf.set("dfs.replication", "2"); 
        Job job = Job.getInstance(conf); 
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);  
        FileInputFormat.setInputPaths(job, new Path(DFS_FULL_PATH_IN + DFS_FILE_IN));
        FileOutputFormat.setOutputPath(job, new Path(DFS_FULL_PATH_OUT));   
        job.waitForCompletion(true);
    }
	
    /**
     * 数据,运算，结果都在远程 (本地必须先导出的Jar包)
     * @param args
     * @throws Exception
     */  
    public static void RemoteCalcRemoteData() throws Exception {
    	//配置服务器
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", FS_DEFAULTFS);
        conf.set("dfs.replication", "2");
		//HDFS-HA
		conf.set("dfs.nameservices", "namecluster1");
		conf.set("dfs.ha.namenodes.namecluster1", "nn1,nn2");
		conf.set("dfs.namenode.rpc-address.namecluster1.nn1", "NameNode01:9000");
		conf.set("dfs.namenode.rpc-address.namecluster1.nn2", "NameNode02:9000");
		conf.set("dfs.client.failover.proxy.provider.namecluster1"
			,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.app-submission.cross-platform", "true");
		//RM非HA 
        //conf.set("yarn.resourcemanager.hostname", YARN_RM_HOSTNAME);
        conf.set("mapreduce.job.jar", DFS_JAR);
        //RM-HA
		conf.set("yarn.resourcemanager.ha.enabled", "true");
		conf.set("yarn.resourcemanager.cluster-id", "yarncluster");
		conf.set("yarn.resourcemanager.ha.rm-ids", "rm1,rm2");
		conf.set("yarn.resourcemanager.hostname.rm1", "DataNode01");
		conf.set("yarn.resourcemanager.hostname.rm2", "DataNode02");        
    	
        //删除历史输出目录
        FileSystem fileSystem = FileSystem.get(conf);  
    	boolean reuslt= fileSystem.exists(new Path(DFS_PATH_OUT));
    	if (reuslt) {
	    	reuslt = fileSystem.delete(new Path(DFS_PATH_OUT), true);
			System.out.println(DFS_PATH_OUT + "   delete..." + reuslt); 
    	}		
    	//计算，输出
        Job job = Job.getInstance(conf); 
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class); 
        FileInputFormat.setInputPaths(job, new Path(DFS_PATH_IN + DFS_FILE_IN));
        FileOutputFormat.setOutputPath(job, new Path(DFS_FULL_PATH_OUT));          
        job.waitForCompletion(true);
    } 
 
}
