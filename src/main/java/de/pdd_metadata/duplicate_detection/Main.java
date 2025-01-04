package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.io.DataReader;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        String input = "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/src/main/java/de/pdd_metadata/duplicate_detection/Abt-Buy.csv";

        DataReader dataReader = new DataReader(input, true, ';', 0, 100);

        Blocking blocking = new Blocking(4, dataReader, 0.7, 4, 2000000);

        try {
            // blocking.findDuplicatesUsingMultipleKeysConcurrently();
            blocking.findDuplicatesUsingMultipleKeysSequential();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
