package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.io.DataReader;
import de.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.pdd_metadata.duplicate_detection.structures.Record;
import de.pdd_metadata.similarity_measures.Levenshtein;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@Getter
@AllArgsConstructor
public class SortedNeighbourhood {
    private Levenshtein levenshtein = new Levenshtein();
    private Sorter sorter;
    private DataReader dataReader;
    private int partitionSize;
    private KeyElementFactory keyElementFactory;
    private int windowSize;
    private int windowInterval;
    private Set<Duplicate> duplicates = new HashSet<>();
    private double threshold;

    public SortedNeighbourhood(DataReader dataReader, int partitionSize, KeyElementFactory keyElementFactory, int windowSize, int windowInterval, double threshold, Sorter sorter) {
        this.dataReader = dataReader;
        this.partitionSize = partitionSize;
        this.keyElementFactory = keyElementFactory;
        this.windowSize = windowSize;
        this.windowInterval = windowInterval;
        this.threshold = threshold;
        this.sorter = sorter;
    }

    public int findDuplicatesIn(HashMap<Integer, Record> records, int[] order) {
        int numDuplicates = 0;

        for (int windowDistance = 1; windowDistance <= this.windowInterval; ++windowDistance) {

            for (int indexPivot = 0; indexPivot < records.size() - windowDistance; ++indexPivot) {
                int indexPartner = indexPivot + windowDistance;
                Record record1 = records.get(order[indexPivot]);
                Record record2 = records.get(order[indexPartner]);

                double value = this.levenshtein.calculateSimilarityOf(record1.values, record2.values);

                Duplicate duplicate = new Duplicate(record1.id, record2.id, Integer.parseInt(record1.values[0]), Integer.parseInt(record2.values[0]));

                boolean newDuplicate = !this.duplicates.contains(duplicate);

                if (value >= threshold && newDuplicate) {
                    this.duplicates.add(duplicate);
                }
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
                            Record record1 = records.get(partitionIndices[indexPivot]);
                            Record record2 = records.get(partitionIndices[indexPartner]);

                            double value = this.levenshtein.calculateSimilarityOf(record1.values, record2.values);


                            Duplicate duplicate = new Duplicate(record1.id, record2.id, Integer.parseInt(record1.values[0]), Integer.parseInt(record2.values[0]));

                            boolean newDuplicate = !this.duplicates.contains(duplicate);

                            if (value >= threshold && newDuplicate) {
                                this.duplicates.add(duplicate);
                            }
                        }
                    }
                }
            }
        }

    }
}
