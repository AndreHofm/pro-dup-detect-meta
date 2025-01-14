package de.pdd_metadata.duplicate_detection;

import de.hpi.isg.pyro.algorithms.Pyro;
import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.PartialFD;
import de.hpi.isg.pyro.model.PartialKey;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.algorithms.hyucc.HyUCC;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.metanome.backend.result_receiver.ResultReceiver;
import de.pdd_metadata.duplicate_detection.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException {
        String dataPath = "./data/";

        String input = dataPath + "cd.csv";

        DataReader dataReader = new DataReader(input, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        getBestAttributes(input, dataReader);

        String resultInput = dataPath + "cd_gold.csv";

        DataReader resultDataReader = new DataReader(resultInput, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        Set<Duplicate> goldResults = resultDataReader.readResultDuplicates();

        Sorter sorter = new Sorter();

        AttributeKeyElementFactory attributeKeyElementFactory = new AttributeKeyElementFactory();

        // Blocking blocking = new Blocking(4, dataReader, 0.7, 4, 2000000, sorter, attributeKeyElementFactory);


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

        try {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            // blocking.findDuplicatesUsingMultipleKeysSequential();
            // blocking.findDuplicatesUsingSingleKey();
            sortedNeighbourhood.findDuplicatesUsingMultipleKeysSequential();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        /*
        Set<Duplicate> missing = goldResults.stream()
                .filter(duplicate -> results.stream().noneMatch(x -> x.getPosRecordId1() == duplicate.getRecordId1() && x.getPosRecordId2() == duplicate.getRecordId2() ||
                        x.getPosRecordId1() == duplicate.getRecordId2() && x.getPosRecordId2() == duplicate.getRecordId1()))
                .collect(Collectors.toSet());

        Set<Duplicate> solltenNichtDrinSein = results.stream()
                .filter(d -> goldResults.stream().noneMatch(x -> d.getPosRecordId1() == x.getRecordId1() && d.getPosRecordId2() == x.getRecordId2() ||
                        d.getPosRecordId1() == x.getRecordId2() && d.getPosRecordId2() == x.getRecordId1()))
                .collect(Collectors.toSet());
         */

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


    }

    private static HashMap<String, Block> runBlocking(DataReader dataReader) throws IOException {
        HashMap<String, Block> blocks = new HashMap<>();
        blocks = dataReader.readBlockForMulti(new int[]{2});
        return blocks;
    }

    private static void getBestAttributes (String path, DataReader dataReader) {
        try {
            ConfigurationSettingFileInput config = new ConfigurationSettingFileInput(path, false, ';', '"', '\\', false, true, 0, true, false, "");

            FileInputGenerator fileInputGenerator = new DefaultFileInputGenerator(config);

            getPyroUCCAndFD(path, fileInputGenerator);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final boolean DETECT_NARY = true;
        private static final int MAX_SEARCH_SPACE_LEVEL = -1;
        private static final int FILE_MAX_ROWS = -1;
    };

    private static void getHyUCC(FileInputGenerator input) throws AlgorithmExecutionException, FileNotFoundException {
        HyUCC hyUCC= new HyUCC();

        hyUCC.setRelationalInputConfigurationValue(HyUCC.Identifier.INPUT_GENERATOR.name(), input);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.NULL_EQUALS_NULL.name(), Parameters.NULL_EQUALS_NULL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.VALIDATE_PARALLEL.name(), Parameters.VALIDATE_PARALLEL);
        hyUCC.setBooleanConfigurationValue(HyUCC.Identifier.ENABLE_MEMORY_GUARDIAN.name(), Parameters.ENABLE_MEMORY_GUARDIAN);
        hyUCC.setIntegerConfigurationValue(HyUCC.Identifier.MAX_UCC_SIZE.name(), Parameters.MAX_SEARCH_SPACE_LEVEL);
        hyUCC.setIntegerConfigurationValue(HyUCC.Identifier.INPUT_ROW_LIMIT.name(), Parameters.FILE_MAX_ROWS);

        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(input));

        hyUCC.setResultReceiver(resultReceiver);

        hyUCC.execute();

        List<Result> results = resultReceiver.fetchNewResults();

        System.out.println(results.size());

        results.stream().map(x -> (UniqueColumnCombination) x).map(UniqueColumnCombination::toString).forEach(System.out::println);
    }

    private static void getHyFD(FileInputGenerator fileInputGenerator, DataReader dataReader) throws AlgorithmExecutionException, FileNotFoundException {
        HyFD hyFD = new HyFD();

        hyFD.setRelationalInputConfigurationValue("INPUT_GENERATOR", fileInputGenerator);

        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));

        hyFD.setResultReceiver(resultReceiver);

        hyFD.execute();

        List<Result> results = resultReceiver.fetchNewResults();

        Set<String> filteredKeys = results.stream().map(x -> (FunctionalDependency) x).map(FunctionalDependency::getDeterminant).map(ColumnCombination::toString).map(x -> x.replace("cd.csv.", "")).collect(Collectors.toSet());

        filteredKeys.forEach(System.out::println);

        String[] attributes = dataReader.getAttributeNames();

        List<String> result = Arrays.stream(attributes).filter(x -> filteredKeys.stream().noneMatch(y -> y.equals(x))).toList();

        //result.forEach(System.out::println);
    }

    private static void getPyroUCCAndFD(String path, FileInputGenerator input) throws AlgorithmExecutionException, FileNotFoundException {
        Pyro pyro = new Pyro();

        System.out.println(pyro.getPropertyLedger().getProperties().keySet());
        System.out.println(pyro.getPropertyLedger().getProperties().get("tableIdentifier"));

        pyro.setRelationalInputConfigurationValue("inputFile", input);
        pyro.setBooleanConfigurationValue("isNullEqualNull", true);

        List<PartialFD> partialFds = new ArrayList<>();

        List<PartialKey> partialUCCs = new ArrayList<>();

        pyro.setFdConsumer(partialFds::add);

        pyro.setUccConsumer(partialUCCs::add);

        pyro.execute();

        List<PartialFD> filteredList = partialFds.stream()
                .filter(fd -> fd.lhs.getColumns().length != 0)
                .toList();

        DataReader dataReader = new DataReader(path, false, ';', 0, 100, StandardCharsets.ISO_8859_1);

        String[] attributes = dataReader.getAttributeNames();

        List<String> filteredKeys = partialUCCs.stream().map(x -> x.vertical.getColumns()).filter(x -> x.length <= 1).map(x -> x[0].getName()).toList();
        //.filter(x -> Arrays.stream(attributes).noneMatch(y -> y.equals(x))).toList();

        List<Double> filteredKeys2 = partialUCCs.stream().map(x -> x.score).toList();

        // filteredKeys2.forEach(System.out::println);

        filteredKeys.forEach(System.out::println);

        List<String> result = Arrays.stream(attributes).filter(x -> filteredKeys.stream().noneMatch(y -> y.equals(x))).toList();
    }

    private static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();

        return relationalInput.columnNames().stream()
                .map(columnName -> new ColumnIdentifier(tableName, columnName))
                .toList();
    }
}
