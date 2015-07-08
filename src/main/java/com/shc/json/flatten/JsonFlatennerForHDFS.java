package com.shc.json.flatten;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shc.cassandra.CassandraHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by uchaudh on 7/7/2015.
 */
public class JsonFlatennerForHDFS {


    private String directory=null;
    private CassandraHandler cassandraHandler=null;
    private FileStatus[] fstat=null;
    private FileSystem fs=null;


    public JsonFlatennerForHDFS(String directoryPath)
    {
        try {

            directory = directoryPath;
            System.out.println("Getting HDFS Conf and Connecting to HDFS for provided directory location...\n");
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            fs = FileSystem.get(new URI("hdfs://inpunpc310350:9000"), conf);
            fstat = fs.listStatus(new Path(directoryPath));
            System.out.println("Connected to HDFS...\n");
            System.out.println("Trying to Connect to Cassandra...\n");
            cassandraHandler = new CassandraHandler();
            if (CassandraHandler.session == null)
                cassandraHandler.getConnection();
            System.out.println("Connected to Cassandra...");
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }


    public void processJson() {
        try {

            CassandraHandler cassandraHandler=new CassandraHandler();
            if(CassandraHandler.session ==null)
                cassandraHandler.getConnection();

            Map<String, String> cassandraColumswithValues= new LinkedHashMap<String, String>();
            List<String> segmentSequence= new ArrayList<String>();


//            File folder = new File("C:\\Users\\uchaudh\\json_data");
//            File folder = new File(directory);
//            File[] listOfFiles = folder.listFiles();
            String rowkey=null;

//            for (File file : listOfFiles) {

            for (int i=0; i< fstat.length;i++)
            {

                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(fstat[i].getPath())));

                ObjectMapper mapper = new ObjectMapper();
                Map<String, Map<String, String>> map = mapper.readValue(br, Map.class);

                for (String key : map.keySet()) {
                    Map<String, String> innerMap = map.get(key);
                    if (key.contains("Transaction Transfer Info Segment-F2")) {
                        rowkey = innerMap.get("Selling Store Number") + "\t" + innerMap.get("Register Number") + "\t" + innerMap.get("Transaction Number") + "\t" + innerMap.get("Transaction Date") + "\t" + innerMap.get("Transaction Time");
                    }
                    String innerVal=new ObjectMapper().writeValueAsString(innerMap);

                    segmentSequence.add("'"+key+"'");

                    String hexString= Bytes.toHexString(innerVal.getBytes(Charset.forName("UTF-8")));

                    System.out.println("Total bytes: " + getStringSizeInBytes(hexString));

                    cassandraColumswithValues.put("'" + key + "'",hexString );
                }
                String mapAsJson = new ObjectMapper().writeValueAsString(cassandraColumswithValues).replace('\"',' ');

                String sqlString="INSERT INTO rtp.npos_segments (rowkey ,crt_ts,lup_ts,segments, segment_sequence) VALUES ('"+rowkey+"', dateof(now()), dateof(now()),"+ mapAsJson + ","+segmentSequence+ " )";

                cassandraHandler.executeQuery(sqlString);
//                    }

                System.out.println("One JSON File Data Successfully Inserted !");
            }

            System.out.println("All JSON Files Processed for this Directory!");
            CassandraHandler.session.close();

        } catch (IOException ex) {

            ex.printStackTrace();
        }
        catch (Exception ex)
        {

        }
    }

    /**
     * Creates individual files for each segment
     * @param fName
     * @param innerMap
     */
    private static void createFlattenFile(String fName, Map<String,String> innerMap) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fName, true));
            for (String segmentIterator : innerMap.keySet()) {
                writer.write(segmentIterator + ": \t" + innerMap.get(segmentIterator) + "");
                writer.newLine();
            }
            writer.close();
        }

        catch (JsonParseException gex) {
            //System.out.println("Error In Writing File :"+ fName);
        } catch (IOException ioex) {

            //To Do
        }


    }


    /**
     *
     * @param selectQuery
     */
    private void getSegments(String selectQuery)
    {
        try {
//            ResultSet rs = new CassandraHandler().executeSelectQuery("select * from rtp.npos_segments where rowkey='01090\t112\t0804\t2015-06-23\t23:45:55'");
            if (CassandraHandler.session == null)
                cassandraHandler.getConnection();
            ResultSet rs = new CassandraHandler().executeSelectQuery(selectQuery);

            System.out.println("Got "+rs.all().size()+" matching rows !");

            for (Row r : rs.all()) {

                System.out.println("Started Iterating!");
                String key = r.getString(0);
                Map<String,ByteBuffer> map = r.getMap("segments", String.class, ByteBuffer.class);
                List<String> segmentsList= r.getList("segment_sequence",String.class);

                for (String k : segmentsList) {
                    byte[] result = new byte[map.get(k).remaining()];
                    map.get(k).get(result);
                    String str = new String(result, "UTF-8");
                    System.out.println(str);
                }
            }
            CassandraHandler.session.close();
        }
        catch (IOException ioe)
        {

        }

    }


    /**
     *
     * @param myStr
     * @return
     */
    private static int getStringSizeInBytes(String myStr)
    {
        CountingOutputStream cos = new CountingOutputStream();

        try {
            Writer writer = new OutputStreamWriter(cos);
            writer.write(myStr);
            writer.flush();
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
        return cos.total;
    }
}


