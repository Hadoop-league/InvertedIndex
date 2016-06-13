package org.demo.index.inverted.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedCoreMR {

    public static class CoreMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            InputSplit inputSplit = context.getInputSplit();
            String fileName = ((FileSplit)inputSplit).getPath().toString();
            
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            Map<String, Integer> freqMap = new HashMap<String, Integer>();
            while (tokenizer.hasMoreTokens()) {
                String label = tokenizer.nextToken();
                
                if (freqMap.containsKey(label)) {
                    freqMap.put(label, freqMap.get(label) + 1);
                } else {
                    freqMap.put(label, 1);
                }
                
                
                context.write(new Text(label), new Text(fileName + "," + freqMap.get(label)));
            }
        }
    }
    
    public static class CoreReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if (null == values) {
                return;
            }
            
            StringBuffer filesBuffer = new StringBuffer();
            boolean firstFlag = true;
            Set<String> filterSet = new HashSet<String>();
            for (Text invertedFile : values) {
                String fileName = invertedFile.toString().split(",")[0];
                int wordFreq = Integer.parseInt(invertedFile.toString().split(",")[1]);
                
                if (filterSet.contains(fileName)) {
                    continue;
                }
                
                filterSet.add(fileName);
                filesBuffer.append((firstFlag ? "" : ", [") + fileName + " : " + wordFreq + "]");
                firstFlag = false;
            }
            
            context.write(key, new Text(filesBuffer.toString()));
        }
    }
}
