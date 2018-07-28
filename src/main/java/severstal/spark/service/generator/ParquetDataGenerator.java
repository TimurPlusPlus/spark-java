package severstal.spark.service.generator;

import severstal.spark.repository.Data;
import severstal.spark.repository.DataRepository;

import java.util.ArrayList;
import java.sql.Date;
import java.util.List;
import java.util.Random;

public class ParquetDataGenerator implements DataGenerator {

    private DataRepository repository;

    public ParquetDataGenerator(DataRepository repository) {
        this.repository = repository;
    }

    @Override
    public void generateData() {
        List<Data> data = new ArrayList<>();
        Random rand = new Random();
        for (int i = 0; i < 10000001; i++) {
            data.add(new Data(rand.nextInt(1001), rand.nextInt(300), new Date(rand.nextLong())));
        }
        repository.write(data);
    }
}
