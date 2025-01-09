package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        String dataPath = "./data/";

        String input = dataPath + "cd.csv";

        DataReader dataReader = new DataReader(input, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        Sorter sorter = new Sorter();

        AttributeKeyElementFactory attributeKeyElementFactory = new AttributeKeyElementFactory();

        Blocking blocking = new Blocking(4, dataReader, 0.7, 4, 2000000, sorter, attributeKeyElementFactory);


        try {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            blocking.findDuplicatesUsingMultipleKeysSequential();
            // blocking.findDuplicatesUsingSingleKey();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        String resultInput = dataPath + "cd_gold.csv";

        DataReader resultDataReader = new DataReader(resultInput, true, ';', 0, 100, StandardCharsets.ISO_8859_1);

        Set<Duplicate> goldResults = resultDataReader.readResultDuplicates();

        Set<Duplicate> results = blocking.getDuplicates();

        Set<Duplicate> missing = goldResults.stream()
                .filter(duplicate -> results.stream().noneMatch(x -> x.getPosRecordId1() == duplicate.getRecordId1() && x.getPosRecordId2() == duplicate.getRecordId2() ||
                        x.getPosRecordId1() == duplicate.getRecordId2() && x.getPosRecordId2() == duplicate.getRecordId1()))
                .collect(Collectors.toSet());


        Set<Duplicate> solltenNichtDrinSein = results.stream()
                .filter(d -> goldResults.stream().noneMatch(x -> d.getPosRecordId1() == x.getRecordId1() && d.getPosRecordId2() == x.getRecordId2() ||
                        d.getPosRecordId1() == x.getRecordId2() && d.getPosRecordId2() == x.getRecordId1()))
                .collect(Collectors.toSet());


        int tp = results.size() - missing.size() - solltenNichtDrinSein.size();
        int fp = solltenNichtDrinSein.size();
        int fn = missing.size();

        System.out.println("Number of Duplicates: " + blocking.getDuplicates().size());
        System.out.println("Number of actual Duplicates: " + goldResults.size());
        System.out.println("True Positive: " + tp);
        System.out.println("False Positive: " + fp);
        System.out.println("False Negative: " + fn);
        System.out.println("Precession: " + (double) tp / (double) (tp + fp));
        System.out.println("Recall: " + (double) tp / (double) (tp + fn));
        System.out.println("F1-Score: " +  (double) (2 * tp) / (double) (2 * tp + fn + fp));

    }
}
