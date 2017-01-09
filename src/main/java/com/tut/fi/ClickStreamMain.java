package com.tut.fi;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Adnan
 * Main class
 * <p>
 * Acronym
 * TS = Time stamp
 */
public class ClickStreamMain {

    //(Session timeout in minutes)session expires in 30 minutes,new session after that.
    private static final int TIMEOUT_IN_MINUTES = 30;
    //(Session timeout in milli seconds)
    private static final int TIMEOUT_IN_MILLI_SEC = TIMEOUT_IN_MINUTES * 60 * 1000;
    private static final String LOG_ENTRY_REGEXP =
            "(\\d+.\\d+.\\d+.\\d+).*\\[(.*) \\].(.*) (\\S*).*\\d+ \\d+ (\\S+) \\\"(.*)\\\"";
    private static final Pattern logEntryPattern = Pattern.compile(LOG_ENTRY_REGEXP);
    private static final String TS_PATTERN = "dd/MMM/yyyy:HH:mm:ss";
    private static final DateTimeFormatter TS_FORMATTER = DateTimeFormat.forPattern(TS_PATTERN);

    /**
     * Mapper code
     */
    public static class clickStreamMapper extends Mapper<Object, Text, Ip_TimeStampKey, Text> {

        private Matcher logEntryMatcher;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            logEntryMatcher = logEntryPattern.matcher(value.toString());

            //if record matches then further process will otherwise it'll skip to next entry of log file.
            if (logEntryMatcher.matches()) {
                //getting required data from entry matcher groups
                String ip = logEntryMatcher.group(1);
                DateTime timestamp = DateTime.parse(logEntryMatcher.group(2), TS_FORMATTER);

                Long unixTS = timestamp.getMillis();
                Ip_TimeStampKey resultKey = new Ip_TimeStampKey(ip, unixTS);
                context.write(resultKey, value);
            }//if END
        }
    } //clickStreamMapper END

    /**
     * Reducer class code
     */
    public static class clickStreamReducer extends Reducer<Ip_TimeStampKey, Text, Ip_TimeStampKey, Text> {

        private Text result = new Text();

        public void reduce(Ip_TimeStampKey key, Iterable<Text> values, Context contxt)
                throws IOException, InterruptedException {

            String sessionId = null;
            Long lastTS = null;
            for (Text value : values) {
                String logRecord = value.toString();
                //incrementing the sessionID if its the first record or more than timeout since last click
                //from a single user
                if (lastTS == null || (key.getUnixTS() - lastTS > TIMEOUT_IN_MILLI_SEC))
                    sessionId = key.getIp() + "+" + key.getUnixTS();

                lastTS = key.getUnixTS();
                result.set(logRecord + " " + sessionId);
                contxt.write(null, result);
            }//for END
        }
    }// clickStreamReducer END

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //In case of running task with jar in command line
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // parsing command line arguments
        if (otherArgs.length != 2) {
            System.err.println("Error: enter input & output path properly");
            System.exit(2);
        }

        //in case of running task in IDE
            /*String[]  otherArgs = new String[2];
            otherArgs[0] = "C:\\Users\\Adnan\\Desktop\\hadoopTesting\\logs.log";
            otherArgs[1] = "C:\\Users\\Adnan\\Desktop\\hadoopTesting\\outputs1";*/

        Job job = Job.getInstance(conf);
        job.setJarByClass(ClickStreamMain.class);
        job.setMapperClass(clickStreamMapper.class);
        job.setReducerClass(clickStreamReducer.class);
        job.setMapOutputKeyClass(Ip_TimeStampKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(IpKeyPartitioner.class);
        job.setGroupingComparatorClass(IpKeyComparator.class);
        job.setSortComparatorClass(EventKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
