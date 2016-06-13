package org.demo.index.inverted.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.demo.index.inverted.core.InvertedCoreMR;

public class InvertedClient  extends Configuration implements Tool {

    public static void main(String[] args) {
        InvertedClient client = new InvertedClient();
        
        try {
            ToolRunner.run(client, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Configuration getConf() {
        return this;
    }

    @Override
    public void setConf(Configuration arg0) {
    }

    @Override
    public int run(String[] args) throws Exception {
        @SuppressWarnings("deprecation")
        Job job = new Job(getConf(), "File Inverted Index");
        job.setJarByClass(InvertedCoreMR.class);
        
        job.setMapperClass(InvertedCoreMR.CoreMapper.class);
        // job.setCombinerClass(InvertedCoreMR.CoreReducer.class);
        job.setReducerClass(InvertedCoreMR.CoreReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
