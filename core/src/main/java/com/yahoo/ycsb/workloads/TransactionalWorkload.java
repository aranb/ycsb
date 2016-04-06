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

import java.util.Properties;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

/**
 * A transactional workload.
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>maxtransactionlength</b>: maximum number of operations per transaction (default 10)
 * <LI><b>transactionlengthdistribution</b>: what distribution should be used to choose the number of operations for
 * each transaction, between 1 and maxtransactionlength (default: uniform)
 * </ul>
 * 
 */
public class TransactionalWorkload extends CoreWorkload {
    int maxtransactionlength;
    IntegerGenerator transactionlength;

    public static final String MAX_TRANSACTION_LENGTH_PROPERTY = "maxtransactionlength";
    public static final long MAX_TRANSACTION_LENGTH_DEFAULT = 10;

    public static final String TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY = "transactionlengthdistribution";
    public static final String TRANSACTION_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

    @Override
    public void init(Properties p) throws WorkloadException {
        String transactionlengthdistrib;
        maxtransactionlength = Integer.parseInt(p.getProperty(MAX_TRANSACTION_LENGTH_PROPERTY,
                MAX_TRANSACTION_LENGTH_DEFAULT + ""));
        transactionlengthdistrib = p.getProperty(TRANSACTION_LENGTH_DISTRIBUTION_PROPERTY,
                TRANSACTION_LENGTH_DISTRIBUTION_DEFAULT);

        if (transactionlengthdistrib.equals("uniform")) {
            transactionlength = new UniformIntegerGenerator(1, maxtransactionlength);
        } else if (transactionlengthdistrib.equals("zipfian")) {
            transactionlength = new ZipfianGenerator(1, maxtransactionlength);
        } else if (transactionlengthdistrib.equals("constant")) {
            transactionlength = new ConstantIntegerGenerator(maxtransactionlength); 
        } else {
            throw new WorkloadException("Distribution \"" + transactionlengthdistrib
                    + "\" not allowed for transaction length");
        }

        super.init(p);
    }

    @Override
    public boolean doTransaction(DB db, Object threadstate) {

        int transactions = transactionlength.nextInt();

        long st = System.nanoTime();
        int res = db.startTransaction();
        
        if (res != 0) // in case getting the timestamp fails - don't do the transaction
        	return false; 
        
        for (int i = 0; i < transactions; ++i) {
            super.doTransaction(db, threadstate);
        }
        // Add a read cycle at the end - for checking performance effect only
        //super.doTransactionRead(db);
        
        db.commitTransaction();

        long en = System.nanoTime();

        Measurements.getMeasurements().measure("TRANSACTION", (int) ((en - st) / 1000));

        return true;
    }

}