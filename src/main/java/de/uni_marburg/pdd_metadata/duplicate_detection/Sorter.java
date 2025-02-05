package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.progressive_blocking.BlockResult;

import java.io.IOException;
import java.util.Set;

public class Sorter {
    private int numRecentDuplicates;
    private Set<BlockResult> recentBlockResults;
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

        this.numRecentDuplicates = magpie.getNumDuplicates();
        this.recentBlockResults = magpie.getBlockResults();
        return magpie.getOrder();
    }
}
