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

package com.yahoo.ycsb.workloads;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * A transactional workload with Singletons.
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>singletonproportion</b>: proportion of operations which are singleton transactions (default 0.5)
 * </ul>
 * 
 */
public class SingletonWorkload extends TransactionalWorkload {
    
    public static final String SINGLETON_PROPORTION_PROPERTY = "singletonproportion";
    public static final String SINGLETON_PROPORTION_PROPERTY_DEFAULT = "0.5";
    public static final String TRUE_SINGLETON_PROPERTY = "truesingleton";
    public static final String TRUE_SINGLETON_PROPERTY_DEFAULT = "true";

    DiscreteGenerator singletonChooser;
    DiscreteGenerator singletonoperationchooser;
    boolean _trueSingleton;
    
    @Override
    public void init(Properties p) throws WorkloadException {
        double singletonproportion = Double.parseDouble(p.getProperty(SINGLETON_PROPORTION_PROPERTY,
        		SINGLETON_PROPORTION_PROPERTY_DEFAULT));
        double readproportion=Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY,READ_PROPORTION_PROPERTY_DEFAULT));
        
        singletonChooser = new DiscreteGenerator();
        if (singletonproportion > 0)
        {
        	singletonChooser.addValue(singletonproportion, "SINGLETON");
        }
        singletonChooser.addValue(1.0-singletonproportion, "TXN");

        super.init(p);
        
        singletonoperationchooser=new DiscreteGenerator();
		if (readproportion>0)
		{
			singletonoperationchooser.addValue(readproportion,"READ");
		}
		singletonoperationchooser.addValue(1.0-readproportion,"UPDATE");
		
		_trueSingleton = Boolean.parseBoolean(p.getProperty(TRUE_SINGLETON_PROPERTY, TRUE_SINGLETON_PROPERTY_DEFAULT));
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {

    	String op=singletonChooser.nextString();
    	
    	if (op.compareTo("SINGLETON")==0)
    	{
    		long st = System.nanoTime();
    		doSingletonTransaction(db, threadstate);
    		long en = System.nanoTime();
    		
    		Measurements.getMeasurements().measure("SINGLETON", (int) ((en - st) / 1000));
    		
    		return true;
    	}
    	return super.doTransaction(db, threadstate);
    }
   
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean doInsert(DB db, Object threadstate)
	{
		int res = db.startTransaction();
        
        if (res != 0) // in case getting the timestamp fails - don't do the transaction
        	return false; 
        for (int i=0; i < Client.batchSize && res == 0; i++)
        	res = db.insert(table,buildKeyName(keysequence.nextInt()),buildValues());
        
        db.commitTransaction();
		
		if (res == 0)
			return true;
		
		return false;
	}


	private boolean doSingletonTransaction(DB db, Object threadstate) {
		String op=singletonoperationchooser.nextString();

		boolean res=false;
		if (op.compareTo("READ")==0)
		{
			res=doSingletonTransactionRead(db);
		}
		else if (op.compareTo("UPDATE")==0)
		{
			res=doSingletonTransactionUpdate(db);
		}
		else
		{
			//should never get here.
			System.out.println("oops. should not get here.");

			System.exit(0);
		}
		
		return res;
		
	}

	private boolean doSingletonTransactionUpdate(DB db) {
		// choose a random key
		int keynum = nextKeynum();
		int res = 0;
		
		String keyname = buildKeyName(keynum);

		HashMap<String, ByteIterator> values;

		if (writeallfields) {
			// new data for all the fields
			values = buildValues();
		} else {
			// update a random field
			values = buildUpdate();
		}
		
		if (_trueSingleton)
			res += db.singletonUpdate(table, keyname,values);
		else {
			res += db.startTransaction();
	        
	        if (res != 0) // in case getting the timestamp fails - don't do the transaction
	        	return false;
	        db.update(table,keyname,values);
	        res += db.commitTransaction();
		}
		return (0 == res);
	}

	private boolean doSingletonTransactionRead(DB db) {
		//choose a random key
		int keynum = nextKeynum();
		int res = 0;

		String keyname = buildKeyName(keynum);

		HashSet<String> fields = null;

		if (!readallfields) {
			// read a random field
			String fieldname = "field" + fieldchooser.nextString();

			fields = new HashSet<String>();
			fields.add(fieldname);
		}
		
		if (_trueSingleton)
			res += db.singletonRead(table, keyname, fields, new HashMap<String, ByteIterator>());
		else {
			res += db.startTransaction();
	        
	        if (res != 0) // in case getting the timestamp fails - don't do the transaction
	        	return false; 
	        db.read(table,keyname,fields,new HashMap<String,ByteIterator>());
	        res += db.commitTransaction();
		}
		return (0 == res);
	}

}