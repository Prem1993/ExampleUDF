
package com.wipro.UDFs;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.*;

import java.security.SecureRandom;

// Description of the UDF
@Description(
        name="ExampleUDF",
        value="returns a lower case version of the input string.",
        extended="select ExampleUDF(deviceplatform) from hivesampletable limit 10;"
)

public class RandomString extends UDF {
    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static SecureRandom rnd = new SecureRandom();

    // Accept a string input
    public String evaluate(String input) {
        // If the value is null, return a null
        if(input == null)
            return null;
        int len=10;
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ )
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        return sb.toString();
    }
}
