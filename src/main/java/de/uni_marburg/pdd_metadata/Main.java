package de.uni_marburg.pdd_metadata;

import de.uni_marburg.pdd_metadata.data_profiling.AttributeWeightProfiler;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeWeight;
import de.uni_marburg.pdd_metadata.duplicate_detection.Blocking;
import de.uni_marburg.pdd_metadata.duplicate_detection.ResultCollector;
import de.uni_marburg.pdd_metadata.duplicate_detection.SortedNeighbourhood;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.utils.Configuration;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.uni_marburg.pdd_metadata.io.EvalWriter.writeCSV;
import static de.uni_marburg.pdd_metadata.io.EvalWriter.writeCSVForInteger;

public class Main {
    final static Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        if (true) {
            for (Configuration.Dataset dataset : Configuration.Dataset.values()) {
                log.info("Dataset: {}", dataset);
                //usingThreshold(dataset);
                execute(dataset);
                System.out.println("___________________________________________________________");
            }
        }

        if (false) {
            var dataset = Configuration.Dataset.NCVOTERS;

            log.info("Dataset: {}", dataset);
            execute(dataset);

            //usingThreshold(dataset);
        }
    }

    private static void execute(Configuration.Dataset dataset) throws Exception {
        log.info("Starting programme...");

        Configuration config = new Configuration();
        config.setDataset(dataset);

        String dataPath = "./data/";
        String input = dataPath + config.getFileName();
        DataReader dataReader = new DataReader(input, config);

        ResultCollector resultCollector = new ResultCollector(dataReader, config);
        Blocking blocking = new Blocking(dataReader, resultCollector, config);
        SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, resultCollector, config);

        if (config.isUSE_PROFILER()) {
            AttributeWeightProfiler profiler = new AttributeWeightProfiler(dataReader, input, config);
            profiler.execute();

            List<AttributeWeight> attributeWeights = profiler.getAttributeWeights();

            HashMap<Integer, AttributeWeight> attributeScoreHashMap = new HashMap<>();
            for (AttributeWeight attributeWeight : attributeWeights) {
                attributeScoreHashMap.put(attributeWeight.getIndex(), attributeWeight);
            }

            int[] attributeIndex = attributeWeights.stream().mapToInt(AttributeWeight::getIndex).toArray();

            sortedNeighbourhood.getLevenshtein().setSimilarityAttributes(attributeIndex);
            sortedNeighbourhood.getLevenshtein().setAttributeWeights(attributeScoreHashMap);

            blocking.getLevenshtein().setSimilarityAttributes(attributeIndex);
            blocking.getLevenshtein().setAttributeWeights(attributeScoreHashMap);
        }

        if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.SNM) {
            sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
        } else if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.BLOCKING) {
            blocking.findDuplicatesUsingMultipleKeysSequential();
        }

        resultCollector.logResults();

        log.info("Ending programme");
    }

    private static void usingThreshold(Configuration.Dataset dataset) throws Exception {
        List<Double> thresholds = new ArrayList<>();
        List<Double> precisions = new ArrayList<>();
        List<Double> recalls = new ArrayList<>();
        List<Double> f1Scores = new ArrayList<>();
        List<Integer> numberOfAttributes = new ArrayList<>();

        Configuration config = new Configuration();
        config.setDataset(dataset);

        for (double i = 0; i <= 1; i = Math.round((i + 0.05) * 100.0) / 100.0) {
            log.info("Starting programme...");

            config.setNullThreshold(i);
            log.info("Current threshold: {}", config.getNullThreshold());

            String dataPath = "./data/";
            String input = dataPath + config.getFileName();
            DataReader dataReader = new DataReader(input, config);

            ResultCollector resultCollector = new ResultCollector(dataReader, config);
            Blocking blocking = new Blocking(dataReader, resultCollector, config);
            SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, resultCollector, config);

            if (config.isUSE_PROFILER()) {
                AttributeWeightProfiler profiler = new AttributeWeightProfiler(dataReader, input, config);
                profiler.execute();

                List<AttributeWeight> attributeWeights = profiler.getAttributeWeights();

                HashMap<Integer, AttributeWeight> attributeScoreHashMap = new HashMap<>();
                for (AttributeWeight attributeWeight : attributeWeights) {
                    attributeScoreHashMap.put(attributeWeight.getIndex(), attributeWeight);
                }

                int[] attributeIndex = attributeWeights.stream().mapToInt(AttributeWeight::getIndex).toArray();

                sortedNeighbourhood.getLevenshtein().setSimilarityAttributes(attributeIndex);
                sortedNeighbourhood.getLevenshtein().setAttributeWeights(attributeScoreHashMap);

                blocking.getLevenshtein().setSimilarityAttributes(attributeIndex);
                blocking.getLevenshtein().setAttributeWeights(attributeScoreHashMap);

                numberOfAttributes.add(attributeWeights.size());
            }

            if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.SNM) {
                sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
            } else if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.BLOCKING) {
                blocking.findDuplicatesUsingMultipleKeysSequential();
            }

            resultCollector.logResults();

            thresholds.add(config.getNullThreshold());
            precisions.add(resultCollector.getPrecision());
            recalls.add(resultCollector.getRecall());
            f1Scores.add(resultCollector.getF1());

            log.info("Ending programme");
        }

        String resultPath = "./results/" + config.getALGORITHM() + "/" + config.getDatasetName() + "/missing_values/";

        writeCSV(resultPath + "missing_values_", thresholds, precisions, recalls, f1Scores, numberOfAttributes);
    }

    private static void maxDet(Configuration.Dataset dataset) throws Exception {
        List<Integer> maxDet = new ArrayList<>();
        List<Double> precisions = new ArrayList<>();
        List<Double> recalls = new ArrayList<>();
        List<Double> f1Scores = new ArrayList<>();
        List<Integer> numberOfAttributes = new ArrayList<>();

        Configuration config = new Configuration();
        config.setDataset(dataset);

        log.info("Dataset: {}", dataset);

        for (int i = 1; i <= 4; i++) {
            log.info("Starting programme...");

            config.setMaxUCCDeterminant(i);
            log.info("Current max determinant: {}", config.getMaxUCCDeterminant());

            String dataPath = "./data/";
            String input = dataPath + config.getFileName();
            DataReader dataReader = new DataReader(input, config);

            ResultCollector resultCollector = new ResultCollector(dataReader, config);
            Blocking blocking = new Blocking(dataReader, resultCollector, config);
            SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, resultCollector, config);

            if (config.isUSE_PROFILER()) {
                AttributeWeightProfiler profiler = new AttributeWeightProfiler(dataReader, input, config);
                profiler.execute();

                List<AttributeWeight> attributeWeights = profiler.getAttributeWeights();

                HashMap<Integer, AttributeWeight> attributeScoreHashMap = new HashMap<>();
                for (AttributeWeight attributeWeight : attributeWeights) {
                    attributeScoreHashMap.put(attributeWeight.getIndex(), attributeWeight);
                }

                int[] attributeIndex = attributeWeights.stream().mapToInt(AttributeWeight::getIndex).toArray();

                sortedNeighbourhood.getLevenshtein().setSimilarityAttributes(attributeIndex);
                sortedNeighbourhood.getLevenshtein().setAttributeWeights(attributeScoreHashMap);

                blocking.getLevenshtein().setSimilarityAttributes(attributeIndex);
                blocking.getLevenshtein().setAttributeWeights(attributeScoreHashMap);

                numberOfAttributes.add(attributeWeights.size());
            }

            if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.SNM) {
                sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
            } else if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.BLOCKING) {
                blocking.findDuplicatesUsingMultipleKeysSequential();
            }

            resultCollector.logResults();

            maxDet.add(config.getMaxFDDeterminant());
            precisions.add(resultCollector.getPrecision());
            recalls.add(resultCollector.getRecall());
            f1Scores.add(resultCollector.getF1());

            log.info("Ending programme");
        }

        String resultPath = "./results/" + config.getALGORITHM() + "/" + config.getDatasetName() + "/ucc/";

        writeCSVForInteger(resultPath + "ucc_max_det_aw_", maxDet, precisions, recalls, f1Scores, numberOfAttributes);
    }
}
