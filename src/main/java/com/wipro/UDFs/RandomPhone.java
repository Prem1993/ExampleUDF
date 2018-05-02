
package com.wipro.UDFs;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

// Description of the UDF
@Description(
        name="ExampleUDF",
        value="returns a lower case version of the input string.",
        extended="select ExampleUDF(deviceplatform) from hivesampletable limit 10;"
)
public class RandomPhone extends UDF {
    // Accept a string input
    public String evaluate(String input) {
        // If the value is null, return a null
        long number = (long) Math.floor(Math.random() * 9_000_000_000L) + 1_000_000_000L;
        String str=Long.toString(number);
        // Lowercase the input string and return it
        return str;
    }
}
