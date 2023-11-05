package org.bigData.Services.Spark;

import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
public class SparkSessionStarter {
    public SparkSession startSession() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        return SparkSession.builder()
                .appName("SparkCSV")
                .master("local[*]")
                .getOrCreate();
    }

}
