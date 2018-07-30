package severstal.spark.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.List;

public interface DataRepository {
    void write(List<Data> data);

    Dataset<Row> read();

    void deleteDataDir() throws IOException;
}
