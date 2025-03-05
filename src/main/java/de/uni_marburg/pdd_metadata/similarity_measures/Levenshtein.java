package de.uni_marburg.pdd_metadata.similarity_measures;

import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeScore;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.IntStream;

@Getter
@Setter
public class Levenshtein {
    // Best for cd {2, 3, 8, 9}
    // Best for dblp_scholar {1, 4}
    // Best for cora {3, 15}
    // Best for Census {1, 5}
    // Best for NCVoters {26, 31, 47}
    private int[] similarityAttributes = {26, 31, 47};
    private int maxAttributeLength;
    private boolean twoInOneDataset;
    private HashMap<Integer, AttributeScore> attributeScores;

    public Levenshtein(int maxAttributeLength, boolean twoInOneDataset) {
        this.maxAttributeLength = maxAttributeLength;
        this.twoInOneDataset = twoInOneDataset;
    }

    public double calculateSimilarityOf(String s1, String s2) {
        if (s1 != null && s2 != null && (!s1.isEmpty() || !s2.isEmpty())) {
            int matrixWidth = Math.min(s1.length(), this.maxAttributeLength);
            int matrixHeight = Math.min(s2.length(), this.maxAttributeLength);
            int[] line1 = IntStream.rangeClosed(0, matrixWidth).toArray();
            int[] line2 = new int[matrixWidth + 1];
            int[] line3;

            for (int i = 0; i < matrixHeight; ++i) {
                line2[0] = i + 1;

                for (int j = 0; j < matrixWidth; ++j) {
                    if (s1.charAt(j) == s2.charAt(i)) {
                        line2[j + 1] = line1[j];
                    } else {
                        line2[j + 1] = 1 + Math.min(line2[j], Math.min(line1[j], line1[j + 1]));
                    }
                }

                line3 = line1;
                line1 = line2;
                line2 = line3;
            }

            return 1 - (double) line1[line1.length - 1] / Math.max(matrixWidth, matrixHeight);
        } else {
            return Double.NaN;
        }
    }

    public double calculateSimilarityOf(String[] r1, String[] r2) {
        if (r1 != null && r2 != null) {
            if (!twoInOneDataset || !r1[2].equals(r2[2])) {
                int numComparisons = 0;
                double recordSimilarity = 0;
                double attributeSimilarity;

                for (int attributeIndex : this.similarityAttributes) {
                    if (r1.length > attributeIndex || r2.length > attributeIndex) {
                        if (r1.length > attributeIndex && r2.length > attributeIndex) {
                            attributeSimilarity = this.calculateSimilarityOf(r1[attributeIndex].toLowerCase(), r2[attributeIndex].toLowerCase());
                        } else {
                            attributeSimilarity = 0;
                        }

                        recordSimilarity += attributeSimilarity * attributeScores.get(attributeIndex).getScore();

                        //recordSimilarity += attributeSimilarity;

                        ++numComparisons;
                    }
                }

                return recordSimilarity; // / numComparisons;
            }

            return 0;
        } else {
            throw new IllegalArgumentException("Records must not be null: r1=" + Arrays.toString(r1) + ", r2=" + Arrays.toString(r2));
        }
    }
}
