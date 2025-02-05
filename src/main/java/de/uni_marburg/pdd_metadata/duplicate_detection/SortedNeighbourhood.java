package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.similarity_measures.Levenshtein;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

@Getter
public class SortedNeighbourhood extends DuplicateDetector {
    private int windowSize;
    private int windowInterval;

    public SortedNeighbourhood(DataReader dataReader, Configuration config) {
        super(dataReader, config);
        this.windowSize = config.getWindowSize();
        this.windowInterval = config.getWindowInterval();
    }

    public int findDuplicatesIn(HashMap<Integer, Record> records, int[] order) {
        int numDuplicates = 0;

        for (int windowDistance = 1; windowDistance <= this.windowInterval; ++windowDistance) {

            for (int indexPivot = 0; indexPivot < records.size() - windowDistance; ++indexPivot) {
                int indexPartner = indexPivot + windowDistance;
                detectDuplicates(records, order, indexPivot, indexPartner);
            }
        }

        return numDuplicates;
    }

    public void findDuplicatesUsingMultipleKeysSequential() throws IOException {
        for (int keyAttributeNumber : this.levenshtein.getSimilarityAttributes()) {
            int[] keyAttributeNumbers = new int[]{keyAttributeNumber};
            int[] order = this.sorter.calculateOrderMagpieProgressive(this.keyElementFactory, keyAttributeNumbers, this.dataReader, this.partitionSize, null, this);
            this.runWindowLinearIncreasing(order);
        }
    }

    private void detectDuplicates(HashMap<Integer, Record> records, int[] order, int indexPivot, int indexPartner) {
        Record record1 = records.get(order[indexPivot]);
        Record record2 = records.get(order[indexPartner]);

        double value = this.levenshtein.calculateSimilarityOf(record1.values, record2.values);

        Duplicate duplicate = new Duplicate(record1.index, record2.index, record1.values[0], record2.values[0]);

        boolean newDuplicate = !this.duplicates.contains(duplicate);

        if (value >= threshold && newDuplicate) {
            this.duplicates.add(duplicate);
        }
    }

    private void runWindowLinearIncreasing(int[] order) throws IOException {
        int localWindowInterval = this.partitionSize - this.windowSize + 1 <= order.length ? this.windowInterval : this.windowSize;

        for (int currentWindowStart = 1; currentWindowStart < this.windowSize; currentWindowStart += localWindowInterval) {
            int currentWindowEnd = Math.min(currentWindowStart + localWindowInterval, this.windowSize);
            if (currentWindowStart > this.windowInterval || localWindowInterval != this.windowInterval) {
                for (int partitionStartIndex = 0; partitionStartIndex < order.length; partitionStartIndex += this.partitionSize - this.windowSize + 1) {
                    int partitionEndIndex = Math.min(partitionStartIndex + this.partitionSize, order.length);
                    int[] partitionIndices = Arrays.copyOfRange(order, partitionStartIndex, partitionEndIndex);
                    HashMap<Integer, Record> records = this.dataReader.readLines(partitionIndices);
                    int lastPivotElement = partitionStartIndex + this.partitionSize - this.windowSize + 1 < order.length ? partitionIndices.length - this.windowSize + 1 : partitionIndices.length;

                    for (int windowDistance = currentWindowStart; windowDistance < currentWindowEnd; ++windowDistance) {

                        for (int indexPivot = 0; indexPivot < lastPivotElement; ++indexPivot) {
                            int indexPartner = indexPivot + windowDistance;
                            if (indexPartner >= partitionIndices.length) {
                                break;
                            }
                            detectDuplicates(records, partitionIndices, indexPivot, indexPartner);
                        }
                    }
                }
            }
        }

    }
}
