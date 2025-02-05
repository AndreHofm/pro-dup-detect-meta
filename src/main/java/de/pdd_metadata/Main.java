package de.pdd_metadata;

import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.pdd_metadata.data_profiling.AttributeScoringProfiler;
import de.pdd_metadata.data_profiling.structures.AttributeScore;
import de.pdd_metadata.duplicate_detection.Blocking;
import de.pdd_metadata.duplicate_detection.SortedNeighbourhood;
import de.pdd_metadata.duplicate_detection.Sorter;
import de.pdd_metadata.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        String cd = "cd";
        String dblp = "dblp_scholar";
        String abtBuy = "Abt_Buy";
        String amazonGoogle = "Amazon_Google";
        String cora = "cora";

        String dataPath = "./data/";

        String input = dataPath + cora + ".csv";

        DataReader dataReader = new DataReader(input, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        String resultInput = dataPath + "cora_DPL.csv";

        DataReader resultDataReader = new DataReader(resultInput, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        Set<Duplicate> goldResults = resultDataReader.readResultDuplicates();

        Sorter sorter = new Sorter();

        AttributeKeyElementFactory attributeKeyElementFactory = new AttributeKeyElementFactory();

        ConfigurationSettingFileInput config = new ConfigurationSettingFileInput(input,
                false,
                ';',
                '"',
                '\\',
                false,
                true,
                0,
                true,
                false,
                "");

        DefaultFileInputGenerator fileInputGenerator = new DefaultFileInputGenerator(config);

        AttributeScoringProfiler profiler = new AttributeScoringProfiler(dataReader, fileInputGenerator);

        profiler.execute();

        Blocking blocking = new Blocking(4, dataReader, 0.7, 4, 2000000, sorter, attributeKeyElementFactory);

        /*
        MultiBlock multiBlock = new MultiBlock();

        BlockingStructure h = new BlockingStructure();

        h.blocks = runBlocking(dataReader);

        System.out.println(h.blocks.size());

        multiBlock.execute2(h);

        System.out.println(multiBlock.duplicates.size());

        goldResults.removeAll(multiBlock.duplicates);

        System.out.println(goldResults.size());

         */

        SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, 2000000, attributeKeyElementFactory, 20, 1, 0.7, sorter);

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

            // results.stream().map(x -> x.getRecordId1() + " " + x.getRecordId2()).forEach(System.out::println);

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
