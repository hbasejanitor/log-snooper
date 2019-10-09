package org.mike.logsnooper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileReaderToKafka {
  private static final Logger LOG = LoggerFactory.getLogger(FileReaderToKafka.class);
  
  public static void main(String[] args) {

    Options options = new Options();

    
    Option op = new Option("k", "kafkabrokers", true, "Bootstrap kafka servers (comma delimited)");
    op.setRequired(true);    
    options.addOption(op);
      
    op = new Option("t", "topic", true, "Topic to publish to");
    op.setRequired(true);    
    options.addOption(op);

    op = new Option("f", "file", true, "File containing the data that will be pushed to the topic");
    op.setRequired(true);    
    options.addOption(op);

    CommandLine commandLine = null;
    
    try {
      commandLine = new BasicParser().parse(options, args);
    } catch (ParseException e) {
      printUsageAndExit(options, -1);
    }
    
    
    String topic = commandLine.getOptionValue('t');

    Properties configProperties = new Properties();

    
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, commandLine.getOptionValue('k'));
    
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    
    try (BufferedReader reader = new BufferedReader(new FileReader(new File(commandLine.getOptionValue('f'))));
        KafkaProducer<String,String> produer= new KafkaProducer<String, String>(configProperties);  ){
      //200.4.91.190 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"
      String line = reader.readLine();
      LogLine lline = new LogLine(line);
      while (line !=null) {
        ProducerRecord<String, String> kect = new ProducerRecord<String, String>(topic,lline.ipAddress, line);
        produer.send(kect);
        line = reader.readLine();
        if (line !=null) {
          lline = new LogLine(line);  
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("java -cp log-snooper-0.0.1-SNAPSHOT.jar org.mike.logsnooper.FloodDetector", "", options,
        "", true);
    System.exit(exitCode);
  }
}
