package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys.KeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.BlockResult;

import java.io.IOException;
import java.util.Set;

public class Sorter {
    private int magpieNumDiscardKeys = 100;

    public int[] calculateOrderMagpieProgressive(KeyElementFactory keyElementFactory, int[] keyAttributeNumbers, DataReader dataReader, int partitionSize, Blocking blocking, SortedNeighbourhood sortedNeighbourhood) throws IOException {
        int numRecords = dataReader.getNumRecords();
        Magpie magpie = new Magpie(numRecords, partitionSize, this.magpieNumDiscardKeys);

        do {
            dataReader.readKeyAndLinesInto(magpie, keyElementFactory, keyAttributeNumbers);

            if (blocking != null) {
                magpie.placeProgressive(blocking, keyAttributeNumbers[0]);
            } else {
                magpie.placeProgressive(sortedNeighbourhood);
            }


        } while (!magpie.isFull());

        return magpie.getOrder();
    }
}
