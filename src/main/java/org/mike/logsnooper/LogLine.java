package org.mike.logsnooper;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

public class LogLine {
  /*
   * 200.4.91.190 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"
209.112.63.162 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; {1C69E7AA-C14E-200E-5A77-8EAB2D667A07})"
209.112.9.34 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/3.01 (compatible;)"

   */

  String ipAddress;
  String timestamp;
  String url;
  String response;
  String contentLength;
  String browser;
  
  public LogLine(String line)  {
    this.ipAddress=line.substring(0,line.indexOf('-')).trim();
    SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
    this.timestamp = line.substring(line.indexOf('[')+1,line.indexOf(']'));
    this.url=line.substring(line.indexOf(']')+2,line.indexOf('"',line.indexOf(']')+3)+1);
    
    String restOf[] = StringUtils.split(line.substring(line.indexOf(this.url)+this.url.length()+1));
    this.response=restOf[0];
    this.contentLength=restOf[1];
    this.browser = StringUtils.join(ArrayUtils.subarray(restOf,3,restOf.length)," ");        
  }
  

  
  
  
  
  
}
