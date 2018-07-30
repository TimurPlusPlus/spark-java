package severstal.spark;

import severstal.spark.repository.DataRepository;
import severstal.spark.repository.ParquetDataRepository;
import severstal.spark.service.generator.DataGenerator;
import severstal.spark.service.generator.ParquetDataGenerator;
import severstal.spark.service.pivot.IPivotService;
import severstal.spark.service.pivot.PivotService;

public class Main {

    public static void main(String[] args) {
        DataRepository repository = new ParquetDataRepository();
        DataGenerator generator = new ParquetDataGenerator(repository);

        //Uncomment to generate data
        //generator.generateData();

        IPivotService pivotService = new PivotService(repository);
        pivotService.pivot();
    }

}
