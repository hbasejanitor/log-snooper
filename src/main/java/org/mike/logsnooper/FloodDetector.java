package org.mike.logsnooper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public class FloodDetector {
  private static final Logger LOG = LoggerFactory.getLogger(FloodDetector.class);

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(" java -cp /tmp/log-snooper-0.0.1-SNAPSHOT.jar org.mike.logsnooper.FloodDetector", "", options,
        "\n", true);
    System.exit(exitCode);
  }
  
  public static void main(String[] args) {
    try {
      Options options = new Options();
      
      Option op = new Option("k", "kafkabrokers", true, "Bootstrap kafka servers (comma delimited)");
      op.setRequired(true);    
      options.addOption(op);
      
      op = new Option("w", "window", true, "Size (in seconds) of the detection window defaults to 1 second");
      op.setRequired(false);    
      options.addOption(op);
      
      op = new Option("t", "topic", true, "Topic to listen on");
      op.setRequired(true);    
      options.addOption(op);
      
      op = new Option("r", "reporting", true, "Number of times a second a IP can appear before being marked as suspicious.  Defaults to 5");
      op.setRequired(false);    
      options.addOption(op);

      op = new Option("f", "reportingfile", true, "File to store suspicious IP addesses in");
      op.setRequired(true);    
      options.addOption(op);

      CommandLine commandLine = null;
      
      try {
        commandLine = new BasicParser().parse(options, args);
      } catch (ParseException e) {
        printUsageAndExit(options, -1);
      }

      String topic = commandLine.getOptionValue('t');
      String brokers = commandLine.getOptionValue('k');
      File logFile = new File(commandLine.getOptionValue('f'));
      
      int windowInSeconds = Integer.parseInt(commandLine.getOptionValue('w',"1"));
      int triggerCount = Integer.parseInt(commandLine.getOptionValue('r',"5"));;
      
      LOG.info("listen on brokers {} topic {} will trigger if I see more than {} lines from a single IP address every {} seconds",brokers, topic,triggerCount,windowInSeconds);

      
      Properties configProperties = new Properties();
      configProperties.put(StreamsConfig.APPLICATION_ID_CONFIG,"flood-detector");
      configProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
      configProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      configProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      
      //KEY == IP address value = record
      //('200.4.91.190','200.4.91.190 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"')
      Cache<String,AtomicLong> tsCounts = Caffeine.newBuilder().maximumSize(60*10).build();
      
      StreamsBuilder b = new StreamsBuilder();
      KStream<String,String> stream = b.stream(topic);
      KStream<String,String> streamKV = stream.map((k,v)->{
        LogLine parsedLogLine = new LogLine(v);
        // we're going to group by IP address & timestamp to find suspicious ones
        return new KeyValue<>(parsedLogLine.ipAddress+","+parsedLogLine.timestamp,v);
      });
      streamKV.groupByKey()
          .windowedBy(TimeWindows.of(1000*windowInSeconds))
          .count()
          .toStream()
          .filter((item,count)->count != null)
          .filter((item,count)->count >= triggerCount)
          .map((window,v)->{
            return new KeyValue<>(StringUtils.split(window.key(),",")[0], StringUtils.split(window.key(),",")[1]);
          }).foreach((k,v)->{
            //at this point, we for each over tuples that are IP,timestamp
            // the IP addresses are suspicious
            // increment the suspicous count for htis time
            AtomicLong counter = tsCounts.get(v, (timestamp)->new AtomicLong(0));
            counter.incrementAndGet();
            // if the timestamp has > 20 suspicious items report it
            if (counter.longValue() > 20) {
              addToSuspiciousFile(logFile, k);
            }
          });
          
      KafkaStreams streamz = new KafkaStreams(b.build(), configProperties);
      streamz.start();
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  public synchronized static void addToSuspiciousFile(File file,String ip) {
    try (BufferedReader read = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      String line = read.readLine();
      while (line !=null) {
        if (line.equals(ip)) {
          // wooohoo, try with resources cleans up
          return;
        }
        line = read.readLine();
      }
    } catch (FileNotFoundException e) {
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    try (RandomAccessFile rfile = new RandomAccessFile(file, "rw")){
      rfile.seek(rfile.length());
      ByteBuffer buff = ByteBuffer.wrap((ip+"\n").getBytes());
      rfile.getChannel().write(buff);
      rfile.getChannel().force(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  

}
