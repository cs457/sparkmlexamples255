
package com.example.springboot;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class HelloController {

    public static List<Double> interceptCoefficent = new ArrayList<>();
    LinearRegressionModel lrModel = null;

    Map<String, String> ageMapping = new HashMap<>();

    @RequestMapping("/")
    public String index() {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("Gym Competitors")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .master("local[*]").getOrCreate();

        System.out.println("spark" + spark);
        Dataset<Row> csvData = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/GymCompetition.csv");

        StringIndexer genderIndexer = new StringIndexer();
        genderIndexer.setInputCol("Gender");
        genderIndexer.setOutputCol("GenderIndex");
        csvData = genderIndexer.fit(csvData).transform(csvData);


        OneHotEncoder encoder = new OneHotEncoder();
        encoder.setInputCols(new String[]{"GenderIndex"});
        encoder.setOutputCols(new String[]{"GenderEncoder"});
        csvData = encoder.fit(csvData).transform(csvData);


        System.out.println("Here ");

        List<Row> list = csvData.select(csvData.col("Gender"), csvData.col("GenderIndex")).distinct().collectAsList();

        for (int k = 0; k < list.size(); k++) {
            System.out.println("losi get()" + list.get(k));
            ageMapping.put(list.get(k).get(0).toString(), list.get(k).get(1).toString());
        }

        csvData.select(csvData.col("GenderIndex"), csvData.col("Gender")).distinct().show();


        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(new String[]{"GenderEncoder", "Age", "Height", "Weight"});

        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(csvData);
        Dataset<Row> modelInputData = csvDataWithFeatures.select("NoOfReps", "features").withColumnRenamed("NoOfReps", "label");
        modelInputData.show(5, false);

        Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingAndTestData = dataSplits[0];
        Dataset<Row> holdOutData = dataSplits[1];
        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
        ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[]{0.01})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMap)
                .setTrainRatio(0.8);


        TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData);
        LinearRegressionModel lrModel = (LinearRegressionModel) model.bestModel();

        lrModel.transform(trainingAndTestData).show(5, false);


        this.lrModel = lrModel;

        System.out.println("*********Details of Model   Accuracy *******");
        System.out.println("coefficient" + lrModel.coefficients());
        System.out.println("intercept" + lrModel.intercept());

        System.out.println("r2: " + lrModel.summary().r2());
        System.out.println("RMSE: " + lrModel.summary().rootMeanSquaredError());
        System.out.println("End Of *********Details of Model Accuracy *******");

        lrModel.transform(holdOutData).show(10, false);

        System.out.println("*********Details of HoldOut Data    Accuracy *******");
        System.out.println("r2: " + lrModel.evaluate(holdOutData).r2());
        System.out.println("RMSE: " + lrModel.evaluate(holdOutData).rootMeanSquaredError());

        System.out.println("End Of *********Details of HoldOut Data    Accuracy *******");

        interceptCoefficent = new ArrayList<>();
        interceptCoefficent.add(lrModel.intercept());

        Double[] d = new Double[lrModel.coefficients().size()];

        for (int i = 0; i < lrModel.coefficients().size(); i++) {
            d[i] = lrModel.coefficients().toArray()[i];
            interceptCoefficent.add(d[i]);
        }


        return interceptCoefficent.toString();
    }

    @GetMapping("/data")
    public @ResponseBody
    ResponseData getTiming(@RequestBody RequestData req) {

        if (lrModel == null) {
            index();
        }

        ResponseData res = new ResponseData();
        res.setModelCreatedTime(new Date());
        System.out.println("Data .............." + req.toString());

        return res;

    }




}
