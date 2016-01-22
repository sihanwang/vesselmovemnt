package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.filedownload;

import javax.xml.soap.*;

import org.apache.commons.io.IOUtils;

import java.io.*;
import java.net.*;

/**
 * @author eivindabusland
 * @created 28.11.14 16:58
 */

public class SoapService {

    /*
     * private static final Logger log = LoggerFactory.getLogger(SoapService.class); private @Value("${soap.connectTimeoutInSeconds:10}") int
     * connectTimeoutInSeconds; private @Value("${soap.readTimeoutInSeconds:10}") int readTimeoutInSeconds; private @Autowired ZookeeperStateDao
     * zookeeperStateDao;
     */

    public SoapService() {
    }

    /*
     * public long getTick(String site) { return zookeeperStateDao.getAccessTime(site); } public void setTick(String site, long ticks) {
     * zookeeperStateDao.setAccessTime(site, ticks); }
     */

    public InputStream sendRequest(SOAPMessage message, String endPoint, final String proxy, String proxyPort) throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        message.writeTo(new PrintStream(baos));

        final Proxy urlproxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("10.23.29.131", 8080));

        URL url = new URL(null, endPoint, new URLStreamHandler() {
            protected URLConnection openConnection(URL url) throws IOException {
                // The url is the parent of this stream handler, so must
                // create clone
                URL clone = new URL(url.toString());

                URLConnection connection = null;

                connection = clone.openConnection(urlproxy);

                connection.setConnectTimeout(10 * 1000);
                connection.setReadTimeout(1000 * 1000);
                // Custom header
                connection.addRequestProperty("Accept-Encoding", "gzip, deflate");
                return connection;
            }
        });

        SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
        SOAPConnection connection = soapConnectionFactory.createConnection();
        SOAPMessage response = connection.call(message, url);

        ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
        response.writeTo(responseStream);

        try (InputStream inputStream = new ByteArrayInputStream(responseStream.toByteArray())) {
            return inputStream;
        }

    }

    /*
     * public void setConnectTimeoutInSeconds(int connectTimeoutInSeconds) { this.connectTimeoutInSeconds = connectTimeoutInSeconds; } public void
     * setReadTimeoutInSeconds(int readTimeoutInSeconds) { this.readTimeoutInSeconds = readTimeoutInSeconds; }
     */
}