package org.mike.logsnooper;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import junit.framework.TestCase;

public class TestLogLine extends TestCase {
  /*
   * 200.4.91.190 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"
209.112.63.162 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; {1C69E7AA-C14E-200E-5A77-8EAB2D667A07})"
209.112.9.34 - - [25/May/2015:23:11:15 +0000] "GET / HTTP/1.0" 200 3557 "-" "Mozilla/3.01 (compatible;)"

   */

  
  public void testLogLineParse() {
    try {
      LogLine line = new LogLine("200.4.91.190 - - [25/May/2015:23:11:15 +0000] \"GET / HTTP/1.0\" 200 3557 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\"");  
      System.out.println(ToStringBuilder.reflectionToString(line,ToStringStyle.MULTI_LINE_STYLE));
      
      assertEquals("200.4.91.190", line.ipAddress);
      assertEquals("25/May/2015:23:11:15 +0000", line.timestamp);
      assertEquals("\"GET / HTTP/1.0\"", line.url);
      assertEquals("200", line.response);
      assertEquals("3557", line.contentLength);
      assertEquals("\"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\"", line.browser);
      
      
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    
    
  }
  
  
  
  
}
