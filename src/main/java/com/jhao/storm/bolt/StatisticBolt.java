package com.jhao.storm.bolt;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;



/**
 * 
 * 将数据存入hdfs
 */
public class StatisticBolt extends BaseBasicBolt {

 

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String ip = input.getStringByField("ip");
        String mid = input.getStringByField("mid");
        byte[] contents = (ip+" "+mid+"\n").getBytes();
        try {
			createFile("hdfs://localhost:9090/tset1/dd.txt", contents);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("---------无法追加到hdfs中---------");
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void createFile(String dst , byte[] contents) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try {
        	//fs = FileSystem.get(URI.create(dst), conf);
        	//要追加的文件流，inpath为文件
        	//InputStream in = new BufferedInputStream();
        	OutputStream out = fs.append(new Path(dst));
        	out.write(contents);
        	out.close();
            fs.close();
            System.out.println("文件创建成功！");
        	//IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
        	e.printStackTrace();
        }
        
       /* 
        Path dstPath = new Path(dst); //目标路径
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");
        */
    }

    
    public StatisticBolt() {
        
    }
}
