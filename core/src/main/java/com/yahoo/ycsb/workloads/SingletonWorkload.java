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

    DiscreteGenerator singletonChooser;
    DiscreteGenerator singletonoperationchooser;
    
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
		int keynum=keysequence.nextInt();
		String dbkey = buildKeyName(keynum);
		HashMap<String, ByteIterator> values = buildValues();
		
		int res = db.startTransaction();
        
        if (res != 0) // in case getting the timestamp fails - don't do the transaction
        	return false; 
        
        res = db.insert(table,dbkey,values);
        
        db.commitTransaction();
		
		if (res == 0)
			return true;
		
		return false;
	}


	private boolean doSingletonTransaction(DB db, Object threadstate) {
		String op=singletonoperationchooser.nextString();

		if (op.compareTo("READ")==0)
		{
			doSingletonTransactionRead(db);
		}
		else if (op.compareTo("UPDATE")==0)
		{
			doSingletonTransactionUpdate(db);
		}
		else
		{
			//should never get here.
			System.out.println("oops. should not get here.");

			System.exit(0);
		}
		
		return true;
		
	}

	private void doSingletonTransactionUpdate(DB db) {
		// choose a random key
		int keynum = nextKeynum();

		String keyname = buildKeyName(keynum);

		HashMap<String, ByteIterator> values;

		if (writeallfields) {
			// new data for all the fields
			values = buildValues();
		} else {
			// update a random field
			values = buildUpdate();
		}

		db.singletonUpdate(table, keyname,values);
	}

	private void doSingletonTransactionRead(DB db) {
		//choose a random key
		int keynum = nextKeynum();

		String keyname = buildKeyName(keynum);

		HashSet<String> fields = null;

		if (!readallfields) {
			// read a random field
			String fieldname = "field" + fieldchooser.nextString();

			fields = new HashSet<String>();
			fields.add(fieldname);
		}

		db.singletonRead(table, keyname, fields, new HashMap<String, ByteIterator>());
	}

}