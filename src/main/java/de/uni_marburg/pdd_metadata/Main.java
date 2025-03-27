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

public class Main {
    final static Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        var dataset = Configuration.Dataset.CD;

        log.info("Dataset: {}", dataset);
        execute(dataset);
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
            AttributeWeightProfiler profiler = new AttributeWeightProfiler(dataReader, input, config, resultCollector);
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

        if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.PSNM) {
            sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
        } else if (config.getALGORITHM() == Configuration.PairSelectionAlgorithm.PB) {
            blocking.findDuplicatesUsingMultipleKeysSequential();
        }

        resultCollector.logResults();
        log.info("Ending programme");
    }
}
