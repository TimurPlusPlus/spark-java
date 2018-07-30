package severstal.spark.repository;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ParquetDataRepository implements DataRepository {

    private JavaSparkContext sc;
    private SQLContext sqlContext;
    private static final String PARQUET_PATH = "src/main/resources/data.parquet";

    public ParquetDataRepository() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Pivot");
        this.sc = new JavaSparkContext(conf);
        this.sqlContext = new SQLContext(sc);
    }

    @Override
    public void write(List<Data> data) {
        JavaRDD rdd = sc.parallelize(data);
        Dataset<Row> df = sqlContext.createDataFrame(rdd, Data.class);
        df.write().partitionBy("id").mode(SaveMode.Append).parquet(PARQUET_PATH);
    }

    @Override
    public Dataset<Row> read() {
        Dataset<Row> df = sqlContext.read().load(PARQUET_PATH);
        return df;
    }

    @Override
    public void deleteDataDir() throws IOException {
        FileUtils.deleteDirectory(new File(PARQUET_PATH));
    }

}
