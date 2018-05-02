
package com.wipro.UDFs;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.sql.Timestamp;

// Description of the UDF
@Description(
        name="ExampleUDF",
        value="returns a random double.",
        extended="select ExampleUDF(deviceplatform) from hivesampletable limit 10;"
)
public class RandomTimestamp extends UDF {
    // Accept a string input
    public Timestamp evaluate(String input) {
        // If the value is null, return a null


        long offset = Timestamp.valueOf("1920-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("1980-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));



       /* Random random = new Random();
        int minDay = (int) LocalDate.of(1900, 1, 1).toEpochDay();
        int maxDay = (int) LocalDate.of(2015, 1, 1).toEpochDay();
        long randomDay = minDay + random.nextInt(maxDay - minDay);

        LocalDate randomBirthDate = LocalDate.ofEpochDay(randomDay);
        Timestamp timestamp = Timestamp.valueOf(String.valueOf(randomBirthDate));
        //System.out.println(randomBirthDate);*/

        return rand;
    }
}
