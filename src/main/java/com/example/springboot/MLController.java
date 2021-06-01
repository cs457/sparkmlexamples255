package com.example.springboot;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

import static org.apache.spark.sql.functions.col;

@RestController
public class MLController {

    public static String INPUT_FILE_PATH = "src/main/GymCompetition.csv";
    private static List<String> nonNumericColumns = new ArrayList<>(Arrays.asList("Gender", "Membership"));
    private static List<String> numericColumns = new ArrayList<>(Arrays.asList("Age", "Height", "Weight"));
    private static String INDEXER = "Indexer";
    private static String ENCODER = "Encoder";
    private static final String OUTPUT_LABEL_COLUMN = "NoOfReps";
    Dataset<Row> holdOutData;
    ResponseData responseDetails = new ResponseData();
    LinearRegressionModel lrModel;
    private List<Double> interceptCofficent = new ArrayList<>();
    private List<String> inputFeatures = new ArrayList<>();
    Map<String, List<String>> mapOfNonNumericAttr = new HashMap<>();


    public static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("Gym Competitors")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
                .master("local[*]").getOrCreate();

        System.out.println("spark" + spark);

        return spark;
    }


    public static Dataset<Row> getDataSetFromInput(SparkSession sparkSession, String inputFilePath) {

        Dataset<Row> csvData = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputFilePath);


        return csvData;


    }

    public Dataset<Row> generateRealTimeDataSet(RequestData requestData) {


        List<DataType> datatype = new ArrayList<DataType>();
        datatype.add(DataTypes.StringType);
        datatype.add(DataTypes.StringType);
        datatype.add(DataTypes.IntegerType);

        datatype.add(DataTypes.IntegerType);
        datatype.add(DataTypes.IntegerType);
        datatype.add(DataTypes.DoubleType);


        List<String> header = new ArrayList<String>();
        List<String> headerList = new ArrayList<>();
        headerList.add("Gender");
        headerList.add("Membership");
        headerList.add("Age");
        headerList.add("Height");
        headerList.add("Weight");
        headerList.add("NoOfReps");


        StructField structField1 = new StructField(headerList.get(0), datatype.get(0), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField2 = new StructField(headerList.get(1), datatype.get(1), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField3 = new StructField(headerList.get(2), datatype.get(2), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField4 = new StructField(headerList.get(3), datatype.get(3), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField5 = new StructField(headerList.get(4), datatype.get(4), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField6 = new StructField(headerList.get(5), datatype.get(5), true, org.apache.spark.sql.types.Metadata.empty());
        List<StructField> structFieldsList = new ArrayList<>();
        structFieldsList.add(structField1);
        structFieldsList.add(structField2);
        structFieldsList.add(structField3);
        structFieldsList.add(structField4);
        structFieldsList.add(structField5);
        structFieldsList.add(structField6);

        StructType schema = new StructType(structFieldsList.toArray(new StructField[0]));

        List<Object> data = new ArrayList<>();


        for(int z=0 ;z<(numericColumns.size() + nonNumericColumns.size());z++)
        {
            data.add(z,"dummy");
        }


        for (int p = 0; p < (numericColumns.size() + nonNumericColumns.size()); p++) {
            int startOfNumericIndex = nonNumericColumns.size();
            int endOfNumericIndex = numericColumns.size() + nonNumericColumns.size();

            for (int k = startOfNumericIndex; k < endOfNumericIndex; k++) {
                data.add(k,1000.0);
            }

           /* data.add("M");
            data.add("Y");
            data.add(21);
            data.add(100);
            data.add(180);
            data.add(50.0);*/


            Set<String> nonNumColumns = mapOfNonNumericAttr.keySet();

            for (String eachCol : nonNumColumns) {
                List<String> listOfVal = mapOfNonNumericAttr.get(eachCol);
                int indexOfColumn = nonNumericColumns.indexOf(eachCol);
                for (String nonNumericVal : listOfVal) {

                    System.out.println("Index Of Column " + indexOfColumn + "Name Of Column" + eachCol);
                    data.add(indexOfColumn, nonNumericVal);

                }

                for (int j = 0; j < startOfNumericIndex; j++) {
                    if (j != indexOfColumn) {
                        data.add(j, "D");
                    }


                }


            }
        }


        //Add Rest Dummy Data


        List<Object> data1 = new ArrayList();
        data1.add("F");
        data1.add("Y");
        data1.add(21);
        data1.add(100);
        data1.add(180);
        data1.add(51.0);


        List<Object> data2 = new ArrayList();
        data2.add("U");
        data2.add("N");
        data2.add(21);
        data2.add(100);
        data2.add(180);
        data2.add(51.0);

        //Gender,Age,Height,Weight,NoOfReps

        List<Row> ls = new ArrayList<Row>();
        Row row = RowFactory.create(data.toArray());
        ls.add(row);
        Row row1 = RowFactory.create(data1.toArray());

        ls.add(row1);

        Row row2 = RowFactory.create(data2.toArray());
        ls.add(row2);


        Dataset<Row> dataset = getSparkSession().createDataFrame(ls, schema);


        return dataset;
    }


    private Dataset<Row> getNumericAndNonNumericDataSet(Dataset<Row> csvData) {


        for (int i = 0; i < nonNumericColumns.size(); i++) {
            StringIndexer genderIndexer = new StringIndexer();
            genderIndexer.setInputCol(nonNumericColumns.get(i));
            genderIndexer.setOutputCol(nonNumericColumns.get(i) + INDEXER);
            csvData = genderIndexer.fit(csvData).transform(csvData);

            csvData.show(20, false);

            OneHotEncoder encoder = new OneHotEncoder();
            encoder.setInputCols(new String[]{nonNumericColumns.get(i) + INDEXER});
            encoder.setOutputCols(new String[]{nonNumericColumns.get(i) + ENCODER});

            csvData = encoder.fit(csvData).transform(csvData);
        }

        csvData.show(10, false);
        return csvData;
    }


    @RequestMapping("/getActualTime")
    public String getActualTime(@RequestBody RequestData requestData) {

        Dataset<Row> realTimeDataSet = generateRealTimeDataSet(requestData);

        realTimeDataSet = getNumericAndNonNumericDataSet(realTimeDataSet);
        inputFeatures = generateInputColumnsFormatting(nonNumericColumns);
        System.out.println("inputFeatures" + inputFeatures.toString());
        realTimeDataSet = getDataSetResults(realTimeDataSet, inputFeatures);
        realTimeDataSet = realTimeDataSet.select(OUTPUT_LABEL_COLUMN, "features").withColumnRenamed(OUTPUT_LABEL_COLUMN, "label");

        Dataset<Row> realTimePredictionDataSet = this.lrModel.transform(realTimeDataSet);

        realTimePredictionDataSet.show(10, true);
        realTimePredictionDataSet = realTimePredictionDataSet.filter(col("label").equalTo(50.0));

        //filter(col("label").equalTo(50.0)).show();

        realTimePredictionDataSet.show(10, false);

        List<Row> finalData = realTimePredictionDataSet.select(col("prediction")).collectAsList();
        Double finalOutput = 0.0;

        for (Row data1 : finalData) {
            finalOutput = (Double) data1.get(0);
        }
        return finalOutput.toString();

    }


    /* @Scheduled(fixedRate = 100000)*/
    @RequestMapping("/getBestModel")
    public @ResponseBody
    ResponseData printDataSet() {

        SparkSession session = getSparkSession();
        Dataset<Row> originalDataSet = null;
        originalDataSet = getDataSetFromInput(session, INPUT_FILE_PATH);
        originalDataSet.show();

        generateDistinctNonNumericMap(originalDataSet);
        Dataset<Row> finalDataSet = getNumericAndNonNumericDataSet(originalDataSet);
        inputFeatures = generateInputColumnsFormatting(nonNumericColumns);
        System.out.println("inputFeatures" + inputFeatures.toString());
        finalDataSet.show(20, false);
        finalDataSet = getDataSetResults(finalDataSet, inputFeatures);

        Dataset<Row> modelInputData = finalDataSet.select(OUTPUT_LABEL_COLUMN, "features").withColumnRenamed(OUTPUT_LABEL_COLUMN, "label");
        Dataset<Row> finalResultDataSet;
        finalResultDataSet = generateBestModelDataSet(modelInputData);
        finalResultDataSet.show();
        finalResultDataSet.select(finalResultDataSet.col("prediction")).show();
        System.out.println("...Final Output ........");
        System.out.println("Coefficient::" + lrModel.coefficients() + "Intercept::" + lrModel.intercept());
        System.out.println("Accuracy R2: " + lrModel.summary().r2() + " RMSE " + lrModel.summary().rootMeanSquaredError());


        return responseDetails;


    }

    private void generateDistinctNonNumericMap(Dataset<Row> originalDataSet) {


        List<Row> eachNonNumericList = new ArrayList<>();
        List<String> eachStringList;
        for (String eachNonNumericCol : nonNumericColumns) {
            eachStringList = new ArrayList<>();

            eachNonNumericList = originalDataSet.select(originalDataSet.col(eachNonNumericCol)).distinct().collectAsList();
            for (int k = 0; k < eachNonNumericList.size(); k++) {
                System.out.println("list get()" + eachNonNumericList.get(k));
                eachStringList.add(eachNonNumericList.get(k).get(0).toString());

            }

            mapOfNonNumericAttr.put(eachNonNumericCol, eachStringList);


        }


    }


    public Dataset<Row> generateBestModelDataSet(Dataset<Row> modelInputData) {

        Dataset<Row>[] dataSplits = modelInputData.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingAndTestData = dataSplits[0];
        holdOutData = dataSplits[1];
        LinearRegression linearRegression = new LinearRegression();
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder();
        ParamMap[] paramMap = paramGridBuilder.addGrid(linearRegression.regParam(), new double[]{0.01, 0.1, 0.5})
                .addGrid(linearRegression.elasticNetParam(), new double[]{0, 0.5, 1})
                .build();

        TrainValidationSplit trainValidationSplit = new TrainValidationSplit()
                .setEstimator(linearRegression)
                .setEvaluator(new RegressionEvaluator().setMetricName("r2"))
                .setEstimatorParamMaps(paramMap)
                .setTrainRatio(0.8);

        trainingAndTestData.show(20, false);


        TrainValidationSplitModel model = trainValidationSplit.fit(trainingAndTestData);

        lrModel = (LinearRegressionModel) model.bestModel();

        System.out.println("Coefficient:: generateBestModel " + lrModel.coefficients() + "Intercept::" + lrModel.intercept());

        lrModel.transform(trainingAndTestData).show(10, false);

        responseDetails.setModelCreatedTime(new Date());

        return lrModel.transform(trainingAndTestData);

    }

    private Dataset<Row> getDataSetResults(Dataset<Row> originalDataSet, List<String> inputFeatures) {

        VectorAssembler vectorAssembler = new VectorAssembler();
        vectorAssembler.setInputCols(inputFeatures.toArray(new String[0]));
        vectorAssembler.setOutputCol("features");
        Dataset<Row> csvDataWithFeatures = vectorAssembler.transform(originalDataSet);

        return csvDataWithFeatures;
    }

    private List<String> generateInputColumnsFormatting(List<String> nonNumericColumns) {

        List<String> nonNumericFeatureList = new ArrayList<>();
        for (int i = 0; i < nonNumericColumns.size(); i++) {

            nonNumericFeatureList.add(nonNumericColumns.get(i) + ENCODER);
        }
        List<String> completeColumn = new ArrayList<>();
        completeColumn.addAll(numericColumns);
        completeColumn.addAll(nonNumericFeatureList);
        return completeColumn;

    }


}
