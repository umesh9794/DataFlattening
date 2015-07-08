package com.shc.json.flatten;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DynamicJsonFlattenerJTest {

    @Before
    public void setUp() throws Exception {
        DynamicJsonFlattener djf=new DynamicJsonFlattener("C:\\Users\\uchaudh\\json_data");
//        djf.processJsonToCassandra();

        djf.getSegments("select transaction_number, selling_store_number,transaction_date from npos_segments where register_number='567' ALLOW FILTERING");

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testProcessJsonToCassandra() throws Exception {

    }
}