/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.yahoo.omid.transaction.STable;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;

/**
 * HBase client for YCSB framework
 */
public class AmosClient extends OmidClient {

	public static final int SingletonWhileTxnContext = -4; // extend error reporting
	
    /**
     * Initialize any state for this DB. Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException {
    }

     /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to read.
     * @param fields
     *            The list of fields to read, or null for all of them
     * @param result
     *            A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int singletonRead(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        // if this is a "new" table, init HTable object. Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try {
                getHTable(table);
                _table = table;
            } catch (IOException e) {
                System.err.println("Error accessing HBase table: " + e);
                return ServerError;
            }
        }

        Result r = null;
        try {
            if (_debug) {
                System.out.println("Doing Singleton read from HBase columnfamily " + _columnFamily);
                System.out.println("Doing read for key: " + key);
            }
            Get g = new Get(Bytes.toBytes(key));
            if (fields == null) {
                g.addFamily(_columnFamilyBytes);
            } else {
                for (String field : fields) {
                    g.addColumn(_columnFamilyBytes, Bytes.toBytes(field));
                }
            }
            if (transactionState == null) {
                r = ((STable) _hTable.getHTable()).singletonGet(g);
            } else {
            	System.err.println("Client performed Singleton Read while in transaction context");
            	return SingletonWhileTxnContext;
            }
        } catch (IOException e) {
            System.err.println("Error doing get: " + e);
            return ServerError;
        }

        for (KeyValue kv : r.raw()) {
            result.put(Bytes.toString(kv.getQualifier()), new ByteArrayByteIterator(kv.getValue()));
            if (_debug) {
                System.out.println("Result for field: " + Bytes.toString(kv.getQualifier()) + " is: "
                        + Bytes.toString(kv.getValue()));
            }

        }
        return Ok;
    }


    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
     * record with the specified record key, overwriting any existing values with the same field name.
     * 
     * @param table
     *            The name of the table
     * @param key
     *            The record key of the record to write
     * @param values
     *            A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int singletonUpdate(String table, String key, HashMap<String, ByteIterator> values) {
        // if this is a "new" table, init HTable object. Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try {
                getHTable(table);
                _table = table;
            } catch (IOException e) {
                System.err.println("Error accessing HBase table: " + e);
                return ServerError;
            }
        }

        if (_debug) {
            System.out.println("Setting up put for key: " + key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            if (_debug) {
                System.out.println("Adding field/value " + entry.getKey() + "/" + entry.getValue() + " to put request");
            }
            p.add(_columnFamilyBytes, Bytes.toBytes(entry.getKey()), entry.getValue().toArray());
        }

        try {
            if (transactionState == null) {
                ((STable) _hTable.getHTable()).singletonPut(p);
            } else {
            	System.err.println("Client performed Singleton Update while in transaction context");
            	return SingletonWhileTxnContext;
            }
        } catch (IOException e) {
            if (_debug) {
                System.err.println("Error doing Singleton put: " + e);
            }
            return ServerError;
        }

        return Ok;
    }

     public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Please specify a threadcount, columnfamily and operation count");
            System.exit(0);
        }

        final int keyspace = 10000; // 120000000;

        final int threadcount = Integer.parseInt(args[0]);

        final String columnfamily = args[1];

        final int opcount = Integer.parseInt(args[2]) / threadcount;

        Vector<Thread> allthreads = new Vector<Thread>();

        for (int i = 0; i < threadcount; i++) {
            Thread t = new Thread() {
                public void run() {
                    try {
                        Random random = new Random();

                        AmosClient cli = new AmosClient();

                        Properties props = new Properties();
                        props.setProperty("columnfamily", columnfamily);
                        props.setProperty("debug", "true");
                        cli.setProperties(props);

                        cli.init();

                        // HashMap<String,String> result=new
                        // HashMap<String,String>();

                        long accum = 0;

                        for (int i = 0; i < opcount; i++) {
                            int keynum = random.nextInt(keyspace);
                            String key = "user" + keynum;
                            long st = System.currentTimeMillis();
                            int rescode;
                            /*
                             * HashMap hm = new HashMap(); hm.put("field1","value1"); hm.put("field2","value2");
                             * hm.put("field3","value3"); rescode=cli.insert("table1",key,hm); HashSet<String> s = new
                             * HashSet(); s.add("field1"); s.add("field2");
                             * 
                             * rescode=cli.read("table1", key, s, result); //rescode=cli.delete("table1",key);
                             * rescode=cli.read("table1", key, s, result);
                             */
                            HashSet<String> scanFields = new HashSet<String>();
                            scanFields.add("field1");
                            scanFields.add("field3");
                            Vector<HashMap<String, ByteIterator>> scanResults = new Vector<HashMap<String, ByteIterator>>();
                            rescode = cli.scan("table1", "user2", 20, null, scanResults);

                            long en = System.currentTimeMillis();

                            accum += (en - st);

                            if (rescode != Ok) {
                                System.out.println("Error " + rescode + " for " + key);
                            }

                            if (i % 1 == 0) {
                                System.out.println(i + " operations, average latency: "
                                        + (((double) accum) / ((double) i)));
                            }
                        }

                        // System.out.println("Average latency: "+(((double)accum)/((double)opcount)));
                        // System.out.println("Average get latency: "+(((double)cli.TotalGetTime)/((double)cli.TotalGetOps)));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            allthreads.add(t);
        }

        long st = System.currentTimeMillis();
        for (Thread t : allthreads) {
            t.start();
        }

        for (Thread t : allthreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
            }
        }
        long en = System.currentTimeMillis();

        System.out.println("Throughput: " + ((1000.0) * (((double) (opcount * threadcount)) / ((double) (en - st))))
                + " ops/sec");

    }
     
     
     public void getHTable(String table) throws IOException {
         synchronized (tableLock) {
             _hTable = new STable(config, table);
         }

     }
}

/*
 * For customized vim control set autoindent set si set shiftwidth=4
 */

