
package com.wipro.UDFs;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

// Description of the UDF
@Description(
        name="ExampleUDF",
        value="returns a random double.",
        extended="select ExampleUDF(deviceplatform) from hivesampletable limit 10;"
)
public class RandomDate extends UDF {
    // Accept a string input
    public Date evaluate(Timestamp input) {
        // If the value is null, return a null
        if(input == null)
            return null;

        Random random = new Random();
        Date startDate = new Date(1900, 1, 1);
        Date endDate =  new Date(2015, 1, 1);

        Date randomDate = new Date(ThreadLocalRandom.current().nextLong(startDate.getTime(), endDate.getTime()));

        return randomDate;
    }
}
