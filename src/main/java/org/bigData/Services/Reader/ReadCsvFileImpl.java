package org.bigData.Services.Reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.ScalaReflection;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

@Service
public class ReadCsvFileImpl implements ReadCsvFile {
    @Override
    public Dataset<Row> entity(SparkSession session, String filename, String filepath,StructType schema) {

        try {
            return session.read().option("header", "true").schema(schema)
                    .csv(filepath + filename);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }
}
