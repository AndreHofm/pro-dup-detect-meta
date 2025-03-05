package de.uni_marburg.pdd_metadata;

import de.uni_marburg.pdd_metadata.data_profiling.AttributeScoringProfiler;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeScore;
import de.uni_marburg.pdd_metadata.duplicate_detection.Blocking;
import de.uni_marburg.pdd_metadata.duplicate_detection.SortedNeighbourhood;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.utils.Configuration;

import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
    final static Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("Starting programme");

        Configuration config = new Configuration();
        config.setDataset(Configuration.Dataset.CENSUS);

        String dataPath = "./data/";

        String input = dataPath + config.getFileName();
        DataReader dataReader = new DataReader(input, config);

        String resultInput = dataPath + config.getGoldStandardFileName();
        DataReader resultDataReader = new DataReader(resultInput, config);

        Set<String> sampleIds = dataReader.readResultDuplicatesSamples();

        Set<Duplicate> goldResults = resultDataReader.readResultDuplicates(sampleIds);

        Blocking blocking = new Blocking(dataReader, config);

        SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, config);

        if (true) {
            AttributeScoringProfiler profiler = new AttributeScoringProfiler(dataReader, input, config);
            profiler.execute();

            List<AttributeScore> attributeScores = profiler.getAttributeScores();

            HashMap<Integer, AttributeScore> attributeScoreHashMap = new HashMap<>();
            for (AttributeScore attributeScore : attributeScores) {
                attributeScoreHashMap.put(attributeScore.getIndex(), attributeScore);
            }

            int[] indices = new int[attributeScores.size()];

            for (int i = 0; i < indices.length; i++) {
                indices[i] = attributeScores.get(i).getIndex();
            }

            var attributeIndex = attributeScores.stream().map(AttributeScore::getIndex).toArray();

            Arrays.sort(attributeIndex);

            System.out.println(Arrays.toString(attributeIndex));

            sortedNeighbourhood.getLevenshtein().setSimilarityAttributes(indices);

            sortedNeighbourhood.getLevenshtein().setAttributeScores(attributeScoreHashMap);

        }

        if (true) {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            // blocking.findDuplicatesUsingMultipleKeysSequential();
            // blocking.findDuplicatesUsingSingleKey();
            sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
            // sortedNeighbourhood.findDuplicatesUsingMultipleKeysConcurrently();
            // sortedNeighbourhood.findDuplicatesUsingSingleKey();

            Set<Duplicate> results = sortedNeighbourhood.getDuplicates();

            Set<Duplicate> fn = new HashSet<>(goldResults);
            fn.removeAll(results);

            Set<Duplicate> fp = new HashSet<>(results);
            fp.removeAll(goldResults);

            printResults(fn, fp, results, goldResults, sortedNeighbourhood);
        }

        log.info("Ending programme");
    }

    private static void printResults(Set<Duplicate> fn, Set<Duplicate> fp, Set<Duplicate> results, Set<Duplicate> goldResults, SortedNeighbourhood snm) {
        int tpSize = results.size() - fp.size();
        int fpSize = fp.size();
        int fnSize = fn.size();

        log.info("Number of Duplicates: {}", snm.getDuplicates().size());
        log.info("Number of actual Duplicates: {}", goldResults.size());
        log.info("True Positive: {}", tpSize);
        log.info("False Positive: {}", fpSize);
        log.info("False Negative: {}", fnSize);
        log.info("Precession: {}", (double) tpSize / (double) (tpSize + fpSize));
        log.info("Recall: {}", (double) tpSize / (double) (tpSize + fnSize));
        log.info("F1-Score: {}", (double) (2 * tpSize) / (double) (2 * tpSize + fnSize + fpSize));
    }
}
