package com.jhao.storm.bolt;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * 日志数据预处理Bolt，
 *     1.提取实现业务需求所需要的信息：ip地址、客户端唯一标识mid
 *     2.发送到下一个Bolt
 */
public class ConvertIPBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        byte[] binary = input.getBinary(0);
        String line = new String(binary);
        String[] fields = line.split("\t");

        if(fields == null || fields.length < 10) {
            return;
        }

        // 获取ip和mid
        String ip = fields[1];
        String mid = fields[2];

     

        // 发送数据到下一个bolt
        collector.emit(new Values(ip, mid));

    }

    /**
     * 定义了发送到下一个bolt的数据包含两个域：province和mid
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip", "mid"));
    }
}
