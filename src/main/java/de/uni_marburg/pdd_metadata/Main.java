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

public class Main {
    final static Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        for (Configuration.Dataset dataset : Configuration.Dataset.values()) {
            //usingThreshold(dataset);
            log.info("Dataset: {}", dataset);
            execute(dataset);
        }



        // execute(Configuration.Dataset.NCVOTERS);
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
            log.info("Current Null-Threshold: {}", config.getNullThreshold());

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

        String resultPath = "./results/" + config.getDatasetName() + "/missing_values_without_pk/";

        writeCSV(resultPath + "missing_values_without_pk_", thresholds, precisions, recalls, f1Scores, numberOfAttributes);
    }
}
