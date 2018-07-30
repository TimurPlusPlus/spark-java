package severstal.spark.service.generator;

import severstal.spark.repository.Data;
import severstal.spark.repository.DataRepository;

import java.util.*;
import java.sql.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class ParquetDataGenerator implements DataGenerator {

    private DataRepository repository;
    private static final int IDS_NUMBER = 1000;
    private static final int VALUES_NUMBER = 1000000;

    public ParquetDataGenerator(DataRepository repository) {
        this.repository = repository;
    }

    @Override
    public void generateData(){
        Random rand = new Random();

        ExecutorService service = Executors.newFixedThreadPool(10);

        long startTime = System.nanoTime();
        //Executors.newCachedThreadPool()

//        for(int id = 0; id < 100; id++) {
//            List<Data> data = new ArrayList<>();
//            for (int j = 0; j < 10000000; j++) {
//                data.add(new Data(rand.nextInt(1000), rand.nextInt(300), new Date(rand.nextLong())));
//            }
//            repository.write(data);
//        }

        IntStream.range(1, IDS_NUMBER + 1).forEach(id -> service.submit(() -> {
            List<Data> data = new ArrayList<>();
            for (int v = 0; v < VALUES_NUMBER; v++) {
                data.add(new Data(id, rand.nextInt(300), new Date(rand.nextLong())));
            }
            repository.write(data);
        }));

        long endTime = System.nanoTime();
        System.out.println("Generate time " + (endTime - startTime) / 1000000.0 / 1000.0);
    }
}
