package org.bigData.Services.Reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public interface ReadCsvFile {
    Dataset<Row> entity(SparkSession session, String filename, String filepath, StructType schema);
}
