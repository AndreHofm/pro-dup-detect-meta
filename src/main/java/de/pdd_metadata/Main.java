package de.pdd_metadata;

import de.hpi.isg.pyro.akka.algorithms.Pyro;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.pdd_metadata.data_profiling.AttributeScoringProfiler;
import de.pdd_metadata.data_profiling.INDProfiler;
import de.pdd_metadata.data_profiling.UCCProfiler;
import de.pdd_metadata.data_profiling.structures.AttributeScore;
import de.pdd_metadata.duplicate_detection.Blocking;
import de.pdd_metadata.duplicate_detection.SortedNeighbourhood;
import de.pdd_metadata.duplicate_detection.Sorter;
import de.pdd_metadata.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        String dataPath = "./data/";

        String input = dataPath + "cd.csv";

        DataReader dataReader = new DataReader(input, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        String resultInput = dataPath + "cd_gold.csv";

        DataReader resultDataReader = new DataReader(resultInput, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        Set<Duplicate> goldResults = resultDataReader.readResultDuplicates();

        Sorter sorter = new Sorter();

        AttributeKeyElementFactory attributeKeyElementFactory = new AttributeKeyElementFactory();

        String input2 = "file:" + "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/data/persons.tsv";
        String input3 = "file:" + "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/data/planets.tsv";


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

        /*
        SortedNeighbourhood sortedNeighbourhood = new SortedNeighbourhood(dataReader, 2000000, attributeKeyElementFactory, 20, 1, 0.7, sorter);

        List<AttributeScore> attributeScores = profiler.getAttributeScores();

        int[] indices = new int[attributeScores.size()];

        for (int i = 0; i < indices.length; i++) {
            indices[i] = attributeScores.get(i).getIndex();
        }

        System.out.println(attributeScores);

        System.out.println(Arrays.toString(indices));

        sortedNeighbourhood.getLevenshtein().setSimilarityAttributes(indices);

        try {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            // blocking.findDuplicatesUsingMultipleKeysSequential();
            // blocking.findDuplicatesUsingSingleKey();
            sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Set<Duplicate> results = sortedNeighbourhood.getDuplicates();

        Set<Duplicate> missing = new HashSet<>(goldResults);
        missing.removeAll(results);

        Set<Duplicate> solltenNichtDrinSein = new HashSet<>(results);
        solltenNichtDrinSein.removeAll(goldResults);


        int tp = results.size() - solltenNichtDrinSein.size();
        int fp = solltenNichtDrinSein.size();
        int fn = missing.size();

        System.out.println("Number of Duplicates: " + sortedNeighbourhood.getDuplicates().size());
        System.out.println("Number of actual Duplicates: " + goldResults.size());
        System.out.println("True Positive: " + tp);
        System.out.println("False Positive: " + fp);
        System.out.println("False Negative: " + fn);
        System.out.println("Precession: " + (double) tp / (double) (tp + fp));
        System.out.println("Recall: " + (double) tp / (double) (tp + fn));
        System.out.println("F1-Score: " + (double) (2 * tp) / (double) (2 * tp + fn + fp));

         */
    }
}
