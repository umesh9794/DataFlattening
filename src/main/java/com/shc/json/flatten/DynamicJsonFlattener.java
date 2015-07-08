package com.shc.json.flatten;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shc.cassandra.CassandraHandler;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by uchaudh on 7/3/2015.
 */
public class DynamicJsonFlattener {

    private String directory=null;
    private CassandraHandler cassandraHandler=null;


    public DynamicJsonFlattener(String directoryPath)
    {
            directory=directoryPath;
        System.out.println("Trying to Connect to Cassandra...\n");

        cassandraHandler=new CassandraHandler();

        if(CassandraHandler.session ==null)
            cassandraHandler.getConnection();
        System.out.println("Connected to Cassandra...");

    }


    public void processJsonToCassandra() {
        try {

            Map<String, String> cassandraColumswithValues= new LinkedHashMap<String, String>();
            List<String> segmentSequence= new ArrayList<String>();
            String sellingStoreNo=null;
            String transactdate=null;
            String registerNumber=null;
            String transactNumber=null;
            String transactTime=null;

//            File folder = new File("C:\\Users\\uchaudh\\json_data");

            System.out.println("Getting Files from Directory...");
            File folder = new File(directory);
            File[] listOfFiles = folder.listFiles();
            String rowkey=null;
            System.out.println("Starting Iteration...");

            for (File file : listOfFiles) {

//                if (file.isFile()) {
//                    File newDirectory = new File("C:\\Users\\uchaudh\\json_data\\flatten_out\\" + file.getName());
//                    if (newDirectory.mkdir() == true)
//                        System.out.println("Directory " + newDirectory.getAbsolutePath() + " Created Successfully!");
//                    else
//                        System.out.println("Error in Creating Directory");

//                FileInputStream fis= new FileInputStream(file);
//                byte[] buf = new byte[4096];
//                UniversalDetector detector = new UniversalDetector(null);
//
//                // (2)
//                int nread;
//                while ((nread = fis.read(buf)) > 0 && !detector.isDone()) {
//                    detector.handleData(buf, 0, nread);
//                }
//                // (3)
//                detector.dataEnd();
//
//                String encoding = detector.getDetectedCharset();
//                if (encoding != null)
//                    System.out.println("Detected encoding = " + encoding);
//                 else
//                    System.out.println("No encoding detected.");

                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Map<String, String>> map = mapper.readValue(file, Map.class);

                    for (String key : map.keySet()) {
                        Map<String, String> innerMap = map.get(key);
                        if (key.contains("Transaction Transfer Info Segment-F2")) {

                            sellingStoreNo=innerMap.get("Selling Store Number");
                            registerNumber=innerMap.get("Register Number");
                            transactNumber= innerMap.get("Transaction Number");
                            transactdate=innerMap.get("Transaction Date");
                            transactTime=innerMap.get("Transaction Time");
                            rowkey = sellingStoreNo + "\t" + registerNumber + "\t" + transactNumber + "\t" + transactdate + "\t" + transactTime;
                        }
                        String innerVal=new ObjectMapper().writeValueAsString(innerMap);

                        segmentSequence.add("'"+key+"'");

                        String hexString=Bytes.toHexString(innerVal.getBytes(Charset.forName("UTF-8")));

                        System.out.println("Total bytes: " + getStringSizeInBytes(hexString));

                        cassandraColumswithValues.put("'" + key + "'",hexString );
                    }
                        String mapAsJson = new ObjectMapper().writeValueAsString(cassandraColumswithValues).replace('\"',' ');

                        String sqlString="INSERT INTO rtp.npos_segments (selling_store_number,transaction_date,register_number,transaction_number,rowkey ,crt_ts,lup_ts,segments, segments_orderlist) VALUES ('"+sellingStoreNo+"','"+transactdate+"','"+registerNumber+"','"+transactNumber+"' , '"+rowkey+"', dateof(now()), dateof(now()),"+ mapAsJson + ","+segmentSequence+ " )";

                    cassandraHandler.executeQuery(sqlString);
//                    }

                System.out.println("One JSON File Data Successfully Inserted !");
                }

            System.out.println("All JSON Files Processed for this Directory!");
            CassandraHandler.session.close();

        } catch (IOException ex) {

            ex.printStackTrace();
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
        } catch (JsonParseException gex) {
            //System.out.println("Error In Writing File :"+ fName);
        } catch (IOException ioex) {

            //To Do
        }
    }


    /**
     *
     * @param selectQuery
     */
    public void getSegments(String selectQuery)
    {
        try {
//            ResultSet rs = new CassandraHandler().executeSelectQuery("select * from rtp.npos_segments where rowkey='01090\t112\t0804\t2015-06-23\t23:45:55'");
            if(CassandraHandler.session ==null)
                cassandraHandler.getConnection();
            ResultSet rs = new CassandraHandler().executeSelectQuery(selectQuery);

          //  System.out.println("Got "+rs.all().size()+" matching rows !\n\n");

            for (Row r : rs.all()) {

                System.out.println("Started Iterating!");
                String transactNumber=r.getString("transaction_number");
                String storeNumber = r.getString(0);
                Date transactDate= r.getDate("transaction_date");
                //String register_number=r.getString("register_number");
//                Map<String,ByteBuffer > map = r.getMap("segments", String.class, ByteBuffer.class);
//                List<String> segmentsList= r.getList("segment_sequence",String.class);

                System.out.println( transactNumber+"\t"+ storeNumber+ "\t"+transactDate+"\t");

//                for (String k : segmentsList) {
//                    byte[] result = new byte[map.get(k).remaining()];
//
//                    map.get(k).get(result);
//
//                    String str = new String(result, "UTF-8");
//
//                    System.out.println(str);
//                }
            }
            CassandraHandler.session.close();
        }
        catch (Exception ioe)
        {
            ioe.printStackTrace();
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

 class CountingOutputStream extends OutputStream {
    int total;

    @Override
    public void write(int i) {
        throw new RuntimeException("don't use");
    }
    @Override
    public void write(byte[] b) {
        total += b.length;
    }

    @Override public void write(byte[] b, int offset, int len) {
        total += len;
    }
}
