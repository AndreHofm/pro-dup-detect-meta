package de.uni_marburg.pdd_metadata;

import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.uni_marburg.pdd_metadata.data_profiling.AttributeScoringProfiler;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeScore;
import de.uni_marburg.pdd_metadata.duplicate_detection.Blocking;
import de.uni_marburg.pdd_metadata.duplicate_detection.SortedNeighbourhood;
import de.uni_marburg.pdd_metadata.duplicate_detection.Sorter;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.utils.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setDataset(Configuration.Dataset.CORA);

        String dataPath = "./data/";

        String input = dataPath + config.getFileName();
        DataReader dataReader = new DataReader(input, config);

        String resultInput = dataPath + config.getGoldStandardFileName();
        DataReader resultDataReader = new DataReader(resultInput, config);

        Set<Duplicate> goldResults = resultDataReader.readResultDuplicates();

        AttributeScoringProfiler profiler = new AttributeScoringProfiler(dataReader, input, config);
        profiler.execute();

        Blocking blocking = new Blocking(dataReader, config);

        SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, config);

        List<AttributeScore> attributeScores = profiler.getAttributeScores();

        int[] indices = new int[attributeScores.size()];

        for (int i = 0; i < indices.length; i++) {
            indices[i] = attributeScores.get(i).getIndex();
        }

        sortedNeighbourhood.getLevenshtein().setSimilarityAttributes(indices);

        if (true) {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            // blocking.findDuplicatesUsingMultipleKeysSequential();
            // blocking.findDuplicatesUsingSingleKey();
            sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();

            Set<Duplicate> results = sortedNeighbourhood.getDuplicates();

            Set<Duplicate> fn = new HashSet<>(goldResults);
            fn.removeAll(results);

            Set<Duplicate> fp = new HashSet<>(results);
            fp.removeAll(goldResults);

            var attributeIndex = attributeScores.stream().map(AttributeScore::getIndex).toArray();

            Arrays.sort(attributeIndex);

            System.out.println(Arrays.toString(attributeIndex));

            printResults(fn, fp, results, goldResults, sortedNeighbourhood);
        }
    }

    private static void printResults(Set<Duplicate> fn, Set<Duplicate> fp, Set<Duplicate> results, Set<Duplicate> goldResults, SortedNeighbourhood snm) {
        int tpSize = results.size() - fp.size();
        int fpSize = fp.size();
        int fnSize = fn.size();

        System.out.println("Number of Duplicates: " + snm.getDuplicates().size());
        System.out.println("Number of actual Duplicates: " + goldResults.size());
        System.out.println("True Positive: " + tpSize);
        System.out.println("False Positive: " + fpSize);
        System.out.println("False Negative: " + fnSize);
        System.out.println("Precession: " + (double) tpSize / (double) (tpSize + fpSize));
        System.out.println("Recall: " + (double) tpSize / (double) (tpSize + fnSize));
        System.out.println("F1-Score: " + (double) (2 * tpSize) / (double) (2 * tpSize + fnSize + fpSize));
    }
}
