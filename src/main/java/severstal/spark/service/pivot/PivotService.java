package severstal.spark.service.pivot;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import severstal.spark.repository.DataRepository;
import static org.apache.spark.sql.functions.*;


public class PivotService implements IPivotService {

    private DataRepository repository;

    public PivotService(DataRepository repository) {
        this.repository = repository;
    }

    @Override
    public void pivot() {
        Dataset<Row> df = repository.read();
        df.select("id", "value").groupBy(col("value")).pivot("id").agg(count(col("id"))).show(20);
        //df.show();
    }
}