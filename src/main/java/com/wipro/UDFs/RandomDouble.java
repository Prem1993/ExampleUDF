
package com.wipro.UDFs;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

// Description of the UDF
@Description(
        name="ExampleUDF",
        value="returns a random double.",
        extended="select ExampleUDF(deviceplatform) from hivesampletable limit 10;"
)
public class RandomDouble extends UDF {
    // Accept a string input
    public double evaluate(Double input) {
        // If the value is null, return a null
        if(input == null)
            return 0;
        double number = (double) Math.floor(Math.random() * 9_000_000_000L) + 1_000_000_000L;
        // Lowercase the input string and return it
        return number;
    }
}
