package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.filedownload;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeMap;

import javax.xml.soap.SOAPException;
import javax.xml.soap.SOAPMessage;

import org.apache.commons.io.IOUtils;







public class SoapDownloader  {


    private SoapService soapService;

    private String proxy;
    private String proxyPort;
    private SOAPMessage message;
    private String endPoint;
    private LinkedHashSet<SoapRequest> requests = new LinkedHashSet<>();


    public SoapDownloader() throws SOAPException {
        this.soapService = new SoapService();
    

    }

    // String strURL2 = "http://energywatch.natgrid.co.uk/EDP-PublicUI/PublicPI/InstantaneousFlowWebService.asmx";
    public SoapDownloader withEndPoint(String endPoint) {
        this.endPoint = endPoint;
        return this;
    }

    public SoapDownloader withMessage(SOAPMessage message) throws SOAPException {
        this.message = message;
        return this;
    }

    public SoapDownloader withRequests(List<SoapRequest> requests) {
        addSingleToRequests();
        for (SoapRequest request : requests) {
            this.requests.add(request);
        }
        return this;
    }

/*    public SoapDownloader withConnectTimeoutInSeconds(int connectTimeoutInSeconds) {
        soapService.setConnectTimeoutInSeconds(connectTimeoutInSeconds);
        return this;
    }

    public SoapDownloader withReadTimeoutInSeconds(int readTimeoutInSeconds) {
        soapService.setReadTimeoutInSeconds(readTimeoutInSeconds);
        return this;
    }

    public long getTick(String site) {
        return soapService.getTick(site);
    }

    public void setTick(String site, long ticks) {
        soapService.setTick(site, ticks);
    }
*/

    //The same as supper.getTextContent(), only return the first if multiple requests.

    public void send() throws Exception {
        addSingleToRequests();
        final StringBuilder sb = new StringBuilder();
        for (final SoapRequest request : requests) {
           InputStream is = soapService.sendRequest(request.getMessage(), request.getEndpoint(), proxy, proxyPort);
           

           BufferedInputStream  isCopy = new BufferedInputStream(is);
                isCopy.mark(Integer.MAX_VALUE);
            String    fullContent = IOUtils.toString(isCopy, "UTF-8");
                isCopy.reset();
      //          tmpAbsolutePath = fileDao.storeInFile(fileSession.getTmpDir(), GuidUtil.toString(GuidUtil.getGuid()), isCopy);


            break;
        }

    }

    public String getTextContent() throws Exception {
        addSingleToRequests();
        StringBuilder sb = new StringBuilder();
        for (final SoapRequest request : requests) {
            InputStream is = soapService.sendRequest(request.getMessage(), request.getEndpoint(), proxy, proxyPort);
            sb.append(IOUtils.toString(is));
            break;
        }
        return sb.toString();

    }




    private void addSingleToRequests() {
        if (message != null && endPoint != null) {
            SoapRequest request = new SoapRequest(message, endPoint);
            this.requests.add(request);
        }
    }
}

class SoapRequest {
    private SOAPMessage message;
    private String endpoint;

    public SoapRequest() {
    }

    public SoapRequest(SOAPMessage message, String endpoint) {
        this.message = message;
        this.endpoint = endpoint;
    }

    public SOAPMessage getMessage() {
        return message;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public SoapRequest setMessage(SOAPMessage message) {
        this.message = message;
        return this;
    }

    public SoapRequest setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    @Override
    public String toString() {
        return endpoint + "-->[-->" + message.toString() + "<--]<--";
    }

    @Override
    public int hashCode() {
        int eCode = endpoint == null ? 0 : endpoint.hashCode();
        int mCode = message == null ? 0 : message.hashCode();
        return eCode << 5 + mCode;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null) {
            return false;
        }
        if (!(object instanceof SoapRequest)) {
            return false;
        }
        SoapRequest request = (SoapRequest) object;
        if (request.message == null || request.endpoint == null) {
            return false;
        }
        return request.message.equals(message) && request.endpoint.equals(endpoint);
    }
}
