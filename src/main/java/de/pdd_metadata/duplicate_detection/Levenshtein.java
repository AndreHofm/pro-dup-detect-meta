package de.pdd_metadata.duplicate_detection;

import lombok.Getter;

import java.util.Arrays;

@Getter
public class Levenshtein {
    private int[] similarityAttributes = new int[]{0, 1, 2};

    public double calculate(final String string1, final String string2) {
        double levenshteinSimilarity = 0;

        int[] upperLine = new int[string1.length() + 1];
        int[] lowerLine;

        for (int i = 0; i <= string1.length(); i++)
            upperLine[i] = i;


        lowerLine = Arrays.copyOf(upperLine, string1.length() + 1);

        for (int j = 1; j <= string2.length(); j++) {
            lowerLine[0] = j;
            for (int i = 1; i <= string1.length(); i++) {
                if (string1.charAt(i - 1) == string2.charAt(j - 1)) {
                    lowerLine[i] = upperLine[i - 1];
                } else {
                    lowerLine[i] = 1 + min(upperLine[i], lowerLine[i - 1], upperLine[i - 1]);
                }
            }
            upperLine = Arrays.copyOf(lowerLine, string1.length() + 1);
        }

        levenshteinSimilarity = 1 - ((double) lowerLine[string1.length()] / Math.max(string1.length(), string2.length()));

        return levenshteinSimilarity;
    }

    public double calculate(final String[] strings1, final String[] strings2) {
        double levenshteinSimilarity = 0;

        int[] upperLine = new int[strings1.length + 1];
        int[] lowerLine;


        for (int i = 0; i <= strings1.length; i++)
            upperLine[i] = i;

        lowerLine = Arrays.copyOf(upperLine, strings1.length + 1);


        for (int j = 1; j <= strings2.length; j++) {
            lowerLine[0] = j;
            for (int i = 1; i <= strings1.length; i++) {
                if (strings1[i - 1].equals(strings2[j - 1])) {
                    lowerLine[i] = upperLine[i - 1];
                } else {
                    lowerLine[i] = 1 + min(upperLine[i], lowerLine[i - 1], upperLine[i - 1]);
                }
            }
            upperLine = Arrays.copyOf(lowerLine, strings1.length + 1);
        }


        levenshteinSimilarity = 1 - ((double) lowerLine[strings1.length] / Math.max(strings1.length, strings2.length));

        return levenshteinSimilarity;
    }

    private static int min(int... numbers) {
        return Arrays.stream(numbers).min().orElse(Integer.MAX_VALUE);
    }
}
