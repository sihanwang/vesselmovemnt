package com.thomsonreuters.bj.bigdatacommunity.hbase.exercise.group1.filedownload;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SaveToHdfs {
    
    public static String workDir = "/user/hdev/0133272/ves/work/";
    public static String archiveDir = "/user/hdev/0133272/ves/archive/";
    public static void main(String[] args) {
        
        moveFile();
/*        // TODO Auto-generated method stub
        String uri="/user/hdev/0133272/test.txt";
        Configuration conf = new Configuration();
    //  conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("hdfs-site.xml"));
        

        

        try {
            FileSystem fs = FileSystem.get( conf);
            Path p = new Path(uri);      
            FSDataOutputStream out = fs.create(p);
            out.write("content".getBytes("UTF-8"));
            out.close();
            System.out.println(fs.getFileStatus(p).getLen());
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }*/
    }
    
    public static void save(String content, String fileName) {

        // TODO Auto-generated method stub
        String uri=  workDir + fileName;
        Configuration conf = new Configuration();
    //  conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("hdfs-site.xml"));
        

        

        try {
            FileSystem fs = FileSystem.get( conf);
            Path p = new Path(uri);      
            FSDataOutputStream fileOut = fs.create(p);
            
            ZipOutputStream zipOut;
            
            zipOut = new ZipOutputStream(fileOut);
            
            ZipEntry ze = new ZipEntry(p.getName());
            zipOut.closeEntry();
            zipOut.putNextEntry(ze);

            zipOut.write(content.getBytes("UTF-8"));
            
            zipOut.finish();
            zipOut.close();
            System.out.println(fs.getFileStatus(p).getLen());
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static void moveFile() {
        int numMoved = 0;
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("core-site.xml"));
            conf.addResource(new Path("hdfs-site.xml"));
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus files[] = hdfs.listStatus(new Path(workDir));
            if (files.length != 0) {
                for (int cnt = 0; cnt < files.length; cnt++) {

                    Path path = files[cnt].getPath();
                    System.out.println("Source File: " + path.toString() );
                    if (!hdfs.rename(files[cnt].getPath(), new Path(archiveDir + path.getName().replace(".csv", ".zip")))) {
                        System.out.println("Error Moving Files to final HDFS Directory [" + archiveDir +path.getName() + "]");
                    } else {
                        numMoved++;
                    }
                }
            } 
        }
        catch (java.io.IOException ioe) {
            System.out.println("Unable to move file to final HDFS Directory.");
            numMoved = 0;
        }
      //  return numMoved;
    }
    
}

