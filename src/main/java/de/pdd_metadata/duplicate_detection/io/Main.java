package de.pdd_metadata.duplicate_detection.io;

import de.pdd_metadata.duplicate_detection.structures.Block;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class Main {
    public static void main(String[] args) throws IOException {
        String input = "/Users/andrehofmann/Documents/Uni/Bachelorarbeit/Code/pro-dup-detect-meta/src/main/java/de/pdd_metadata/duplicate_detection/io/iris.csv";

        DataReader dataReader = new DataReader(input, false, ',', 0, 100);

        int[] ar = new int[150];

        for(int i = 0; i < 150; i++) {
            ar[i] = i;
        }

        HashMap<Integer, Block> data = dataReader.readBlocks(ar, 0, 30,5);
        // System.out.println(Arrays.toString(data.get(1).records));
        // System.out.println(Arrays.toString(data.get(0).records.get(4).values));
    }
}
