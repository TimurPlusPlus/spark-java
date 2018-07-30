package severstal.spark;

import severstal.spark.repository.DataRepository;
import severstal.spark.repository.ParquetDataRepository;
import severstal.spark.service.generator.DataGenerator;
import severstal.spark.service.generator.ParquetDataGenerator;
import severstal.spark.service.pivot.PivotService;
import severstal.spark.service.pivot.DefaultPivotService;

public class Main {

    public static void main(String[] args) {
        DataRepository repository = new ParquetDataRepository();
        DataGenerator generator = new ParquetDataGenerator(repository);

        //Uncomment to generate data
        //generator.generateData();

        PivotService pivotService = new DefaultPivotService(repository);
        pivotService.pivot();
    }

}
