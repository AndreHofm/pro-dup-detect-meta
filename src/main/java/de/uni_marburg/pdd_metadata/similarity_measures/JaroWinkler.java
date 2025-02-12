package de.uni_marburg.pdd_metadata.similarity_measures;

import org.apache.commons.text.similarity.JaroWinklerSimilarity;

public class JaroWinkler {
    private int[] similarityAttributes = {2, 3, 8, 9};

    public double calculate(String[] r1, String[] r2) {
        JaroWinklerSimilarity jaroWinkler = new JaroWinklerSimilarity();

        int numComparisons = 0;
        double recordSimilarity = 0;
        if (!r1[5].equals(r2[5])) {
            for (int attribute : similarityAttributes) {
                double attributeSimilarity = jaroWinkler.apply(r1[attribute].toLowerCase(), r2[attribute].toLowerCase());

                recordSimilarity += attributeSimilarity;
                ++numComparisons;
            }
        }

        return recordSimilarity / numComparisons;
    }
}
