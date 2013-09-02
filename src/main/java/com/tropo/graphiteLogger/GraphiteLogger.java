package com.tropo.graphiteLogger;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class GraphiteLogger {
  private static org.apache.log4j.Logger LOGGER = Logger.getLogger(GraphiteLogger.class);

  private String graphiteHost;
  private int graphitePort;
  private String nodeIdentifier;

  public GraphiteLogger(String graphiteHost, int graphitePort) {
    this.graphiteHost = graphiteHost;
    this.graphitePort = graphitePort;
    try {
      this.nodeIdentifier = java.net.InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ex) {
      LOGGER.warn("Failed to determine host name", ex);
    }
    if (this.graphiteHost == null || this.graphiteHost.length() == 0 || this.nodeIdentifier == null
        || this.nodeIdentifier.length() == 0 || this.graphitePort < 0) {
      LOGGER
          .warn("Faild to create GraphiteLogger, graphiteHost graphitePost or nodeIdentifier could not be defined properly: "
              + about());

    }
  }

  public String getGraphiteHost() {
    return graphiteHost;
  }

  public void setGraphiteHost(String graphiteHost) {
    this.graphiteHost = graphiteHost;
  }

  public int getGraphitePort() {
    return graphitePort;
  }

  public void setGraphitePort(int graphitePort) {
    this.graphitePort = graphitePort;
  }

  public final String about() {
    return new StringBuffer().append("{ graphiteHost=").append(this.graphiteHost).append(", graphitePort=")
        .append(this.graphitePort).append(", nodeIdentifier=").append(this.nodeIdentifier).append(" }").toString();
  }

  public void logToGraphite(String key, long value) {
    Map<String, Long> stats = new HashMap<String, Long>();
    stats.put(key, value);
    logToGraphite(stats);
  }

  public void logToGraphite(Map<String, Long> stats) {
    if (stats.isEmpty()) {
      return;
    }
    try {
      logToGraphite(this.nodeIdentifier, stats);
    } catch (Throwable t) {
      System.out.println("Can't log to graphite " + t.getMessage());
    }
  }

  @SuppressWarnings("rawtypes")
  private void logToGraphite(String nodeIdentifier, Map<String, Long> stats) throws Exception {
    Long curTimeInSec = System.currentTimeMillis() / 1000;
    StringBuffer lines = new StringBuffer();
    for (Map.Entry entry : stats.entrySet()) {
      String key = nodeIdentifier + "." + entry.getKey();
      lines.append(key).append(" ").append(entry.getValue()).append(" ").append(curTimeInSec).append("\n"); // even
                                                                                                            // the
                                                                                                            // last
                                                                                                            // line
                                                                                                            // in
                                                                                                            // graphite
    }
    logToGraphite(lines);
  }

  private void logToGraphite(StringBuffer lines) throws Exception {
    String msg = lines.toString();
    System.out.println("Writing [" + msg.replaceAll("\\r|\\n", "") + "] to graphite ");
    Socket socket = new Socket(graphiteHost, graphitePort);
    try {
      Writer writer = new OutputStreamWriter(socket.getOutputStream());
      writer.write(msg);
      writer.flush();
      writer.close();
    } finally {
      socket.close();
    }
  }
}
