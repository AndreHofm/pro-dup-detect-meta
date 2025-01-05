package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        String input = "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/src/main/java/de/pdd_metadata/duplicate_detection/cddb.csv";

        DataReader dataReader = new DataReader(input, true, ';', 0, 100);

        Sorter sorter = new Sorter();

        AttributeKeyElementFactory attributeKeyElementFactory = new AttributeKeyElementFactory();

        Blocking blocking = new Blocking(4, dataReader, 0.7, 4, 2000000, sorter, attributeKeyElementFactory);

        try {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            blocking.findDuplicatesUsingMultipleKeysSequential();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String resultInput = "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/src/main/java/de/pdd_metadata/duplicate_detection/cddb_DPL.csv";

        DataReader resultDataReader = new DataReader(resultInput, true, ';', 0, 100);

        Set<Duplicate> result = resultDataReader.readResultDuplicates();

        // blocking.getDuplicates().stream().map(x -> x.getRecordId1() + " " + x.getRecordId2()).forEach(System.out::println);

        Set<Duplicate> test = blocking.getDuplicates();


        Set<Duplicate> fehlen = result.stream()
                .filter(duplicate -> test.stream().noneMatch(x -> x.getPosRecordId1() == duplicate.getRecordId1() && x.getPosRecordId2() == duplicate.getRecordId2() ||
                        x.getPosRecordId1() == duplicate.getRecordId2() && x.getPosRecordId2() == duplicate.getRecordId1()))
                .collect(Collectors.toSet());


        Set<Duplicate> solltenNichtDrinSein = test.stream()
                .filter(d -> result.stream().noneMatch(x -> d.getPosRecordId1() == x.getRecordId1() && d.getPosRecordId2() == x.getRecordId2() ||
                        d.getPosRecordId1() == x.getRecordId2() && d.getPosRecordId2() == x.getRecordId1()))
                .collect(Collectors.toSet());

        System.out.println(solltenNichtDrinSein.size());

        solltenNichtDrinSein.stream().map(x -> x.getPosRecordId1() + " " + x.getPosRecordId2()).forEach(System.out::println);

        System.out.println("_____________________________________________________________");

        fehlen.stream().map(x -> x.getRecordId1() + " " + x.getRecordId2()).forEach(System.out::println);

        System.out.println(fehlen.size());
    }

    public static void replaceSeparatorInCSV(String inputFilePath, String outputFilePath, char oldSeparator, char newSeparator) throws IOException {
        try (
                BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath));
                BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Ersetzen des Separators
                String replacedLine = line.replace(oldSeparator, newSeparator);
                writer.write(replacedLine);
                writer.newLine();
            }
        }
    }
}
