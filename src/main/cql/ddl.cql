
cqlsh:rtp> create table npos_segments(
rowkey text,
selling_store_number text,
register_number text,
transaction_number text,
transaction_date timestamp,
segments map<text,blob>,
crt_ts timestamp,
lup_ts timestamp,
segments_orderlist list<text>,
PRIMARY KEY ((selling_store_number,transaction_date),register_number,transaction_number,rowkey) );

--Selecting based on partitioned columns--

cqlsh:rtp> select * from npos_segments where selling_store_number transaction_date='2015-06-24';

--Selecting based on key column of composite Primary key--

cqlsh:rtp> select * from npos_segments where register_number='900' ALLOW FILTERING; // 'ALLOW FILTERING' IS NOT A PREFERRED WAY FOR QUERYING

