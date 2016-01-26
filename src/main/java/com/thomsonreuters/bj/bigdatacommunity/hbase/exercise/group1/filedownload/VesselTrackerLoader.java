package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.filedownload;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.TimeZone;
import java.util.zip.GZIPInputStream;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.soap.MessageFactory;
import javax.xml.soap.SOAPBody;
import javax.xml.soap.SOAPBodyElement;
import javax.xml.soap.SOAPConstants;
import javax.xml.soap.SOAPElement;
import javax.xml.soap.SOAPEnvelope;
import javax.xml.soap.SOAPMessage;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.apache.log4j.Logger;

import com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.ImportVTLocationFromFileWithReducer;


public class VesselTrackerLoader {

    private static final String INITURL = "http://webservice.vesseltracker.com:80/webservices/GlobalSatExport";
    private static final String WS_NS = "http://webservice.vesseltracker.com/";
    public static Logger log;
    public static String cfgPath = "/home/hdev/8003662/hbase/lib/conf";
    public static boolean initTick = true;
    private String lastTick;


    public VesselTrackerLoader() {

                
            
        String loggingCfg = cfgPath + "/logging.conf";
        PropertyConfigurator.configure(loggingCfg);

        log = Logger.getLogger("VesselTrackerLoader");
        log.info("start");
        

    }
    public long readTick() throws IOException {

        if (initTick == true)
        {
            initTick = false;
            return 0;
            
        }
        String path = cfgPath + "/Tick.txt";
        File file = new File(path);
        InputStream input = new FileInputStream(file);
        try {
            byte[] b = new byte[(int) file.length()];
            input.read(b);
            return Long.parseLong(new String(b, Charset.forName("UTF-8")));
        } finally {
            input.close();
        } 

    }
    
    public void wirteTick(long tick) throws IOException {

        String path = cfgPath + "/Tick.txt";
            
            
            FileWriter fw = new FileWriter(path);
            fw.write(Long.toString(tick));
            fw.close();
  

     
    }

    public boolean load() throws Exception {


        log.info("start load");
        SoapDownloader soapDownloader = new SoapDownloader();
        //     long lastTick = soapDownloader.getTick(ZOOKEEPER_PATH);
        //      String startDate_offset = message.getParameterValue("startdate_offset");
        /*
         * if(startDate_offset != null && !startDate_offset.equals("0")) { lastTick = Long.parseLong(startDate_offset); } // DateTime TickTime=new
         * DateTime(lastTick); // DateTime OneHourBefore=new DateTime().minusMinutes(15); if (TickTime.isAfter(OneHourBefore)) {
         * lastTick=OneHourBefore.getMillis(); }
         */

        String usr = "thomsonreutersprod";
        String pwd = "9gr39j";
        long tick = readTick();
       
        SOAPMessage soapMessage = generateMessage(tick, usr, pwd);
        byte[] byteContent = soapDownloader.withMessage(soapMessage).withEndPoint(INITURL)
        //  .withReadTimeoutInSeconds(1000)
                .getBytesContent();
        byte[] XMLContentBytes = unGzip(byteContent);

        InputStream byteStream = new ByteArrayInputStream(XMLContentBytes);
//      FileWriter fw = new FileWriter("D:/response.txt");  
//      fw.write(content);
//      fw.flush();

      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      InputSource is = new InputSource(byteStream);
        Document document = builder.parse(is);
   
        Element root = document.getDocumentElement();
        org.w3c.dom.Node ret;
        String timeCreated;
        try {
            ret = root.getFirstChild().getFirstChild().getFirstChild();
            timeCreated = ret.getAttributes().getNamedItem("timeCreated").getTextContent();
        } catch (NullPointerException e) {
            log.info("No more data");
            return false;
        }


        String fileName = "VTCurrentLocation_" + timeCreated.replace(':', '_').replace('.', '_') + "-" 
                + lastTick.replace(':', '_').replace('.', '_') + ".csv"; 
        log.info("dowloading finish " + fileName);
        long  currentTick = DateTime.parse(timeCreated).getMillis();

        wirteTick(currentTick);


        if (ret == null) {
            log.info("No more data");
            return false;
        }

        String csv = getCSV(ret);
        if (csv == null || csv.length() == 0) {
            log.info("empty");
            return false;
        }

        SaveToHdfs.save(csv, fileName);
        return true;
        
  //      String commandStr = "java -classpath $(hadoop classpath):VesselMovement-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.locationload.reducersideload.ImportVTLocationFromFileWithReducer -conf /etc/hbase/conf/hbase-site.xml -files VesselZone 8003662/vessellocation_small 10";  
        //String commandStr = "ipconfig";  
  //      Process p = Runtime.getRuntime().exec(commandStr);  
      //  ImportVTLocationFromFileWithReducer.run();
  //      SaveToHdfs.moveFile();
/*        Path path = Paths.get(storagePath, fileName);

        try (InputStream bais = new ByteArrayInputStream(csv.getBytes("UTF-8"))) {

            Files.copy(bais, path);
        }*/
    }

public void moveFile()
{
    SaveToHdfs.moveFile();
    log.info("sleep");
}
    /*
     * downloadHelper.withModifiedStringContent(fileName, csv) .withAdditionalInfo(fileName, "aNDA", "VesselTracker") //.withArchive() .send();
     */

    private String getCSV(org.w3c.dom.Node ret) throws IOException {
        StringWriter sw = new StringWriter();
        VesselCSV writer = new VesselCSV(sw, ',', '"', '\\');
        NodeList nl = ret.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            org.w3c.dom.Node n = nl.item(i);
            String[] values = {
                    //Static-Data
                    getValue(n, "shipId"),
                    getValue(n, "sImo"),
                    getValue(n, "sMmsi"),
                    getValue(n, "sCallsign"),
                    getValue(n, "sShiptype"),
                    getValue(n, "sLength"),
                    getValue(n, "sWidth"),
                    getValue(n, "sName"),

                    //Voyage Data
                    getValue(n, "vDraught"),
                    getValue(n, "vDest"),
                    getValue(n, "vDestCleaned"),
                    getValue(n, "vDestLocode"),
                    getValue(n, "vEta"),
                    getValue(n, "vTime"),
                    getValue(n, "vSource"),

                    //Positin Data
                    getValue(n, "pLong"),
                    getValue(n, "pLat"),
                    getValue(n, "pHdg"),
                    getValue(n, "pSpeed"),
                    getValue(n, "pStatus"),
                    getValue(n, "pStatusVt"),
                    getValue(n, "pTime"),
                    getValue(n, "pCourse"),
                    getValue(n, "pSource")
            };
            writer.writeNext(values);
        }
        writer.close();

        return sw.toString();
    }

    private String getValue(org.w3c.dom.Node vessel, String name) {
        String res = null;
        try {
            res = vessel.getAttributes().getNamedItem(name).getTextContent();
        } catch (Exception e) {
        }
        return res;
    }

    private SOAPMessage generateMessage(long tick, String usr, String pwd) throws Exception {
        SOAPMessage soapMessage = MessageFactory.newInstance(SOAPConstants.SOAP_1_1_PROTOCOL).createMessage();
        SOAPEnvelope soapEnvelope = soapMessage.getSOAPPart().getEnvelope();
        soapEnvelope.setPrefix("soapenv");
        soapEnvelope.getHeader().setPrefix("soapenv");
        soapEnvelope.addNamespaceDeclaration("web", WS_NS);
        SOAPBody soapBody = soapEnvelope.getBody();
        soapBody.setPrefix("soapenv");
        SOAPBodyElement soapBodyElement = soapBody.addBodyElement(new QName(WS_NS, "exportGlobalSatData", "web"));
        SOAPElement soapElement = soapBodyElement.addChildElement(new QName("arg0"));
        //soapElement.setValue("thomsonreutersprod");
        soapElement.setValue(usr);
        soapElement = soapBodyElement.addChildElement(new QName("arg1"));
        //soapElement.setValue("9gr39j");
        soapElement.setValue(pwd);
        soapElement = soapBodyElement.addChildElement(new QName("arg2"));
        DateTime dt;
        if (tick > 0) {
            dt = new DateTime(tick).withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT")));
        } else {
            dt = new DateTime().withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("GMT"))).minusMinutes(10);
        }
        lastTick = dt.toString();
        log.info("lats tick = "+ lastTick);
        
        
        soapElement.setValue(dt.toString());

        return soapMessage;
    }
    
    private byte[] unGzip(byte[] gzipXMLContentBytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try{
            IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(gzipXMLContentBytes)), out);
        }catch(java.util.zip.ZipException ex){
            return null;
        } catch(IOException e){
            throw new RuntimeException(e);
        }
        return out.toByteArray();
    }    
}
