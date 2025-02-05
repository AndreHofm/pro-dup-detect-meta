package de.uni_marburg.pdd_metadata.similarity_measures;

import de.uni_marburg.pdd_metadata.similarity_measures.helper.Tokenizer;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import lombok.AllArgsConstructor;

import java.util.*;

@AllArgsConstructor
public class Jaccard {

    // The tokenizer that is used to transform string inputs into token lists.
    private final Tokenizer tokenizer;

    // A flag indicating whether the Jaccard algorithm should use set or bag semantics for the similarity calculation.
    private final boolean bagSemantics;

    /**
     * Calculates the Jaccard similarity of the two input strings. Note that the Jaccard similarity may use set or
     * multiset, i.e., bag semantics for the union and intersect operations. The maximum Jaccard similarity with
     * multiset semantics is 1/2 and the maximum Jaccard similarity with set semantics is 1.
     * @param string1 The first string argument for the similarity calculation.
     * @param string2 The second string argument for the similarity calculation.
     * @return The multiset Jaccard similarity of the two arguments.
     */

    public double calculate(String string1, String string2) {
        string1 = (string1 == null) ? "" : string1;
        string2 = (string2 == null) ? "" : string2;

        String[] strings1 = this.tokenizer.tokenize(string1);
        String[] strings2 = this.tokenizer.tokenize(string2);
        return this.calculate(strings1, strings2);
    }

    /**
     * Calculates the Jaccard similarity of the two string lists. Note that the Jaccard similarity may use set or
     * multiset, i.e., bag semantics for the union and intersect operations. The maximum Jaccard similarity with
     * multiset semantics is 1/2 and the maximum Jaccard similarity with set semantics is 1.
     * @param strings1 The first string list argument for the similarity calculation.
     * @param strings2 The second string list argument for the similarity calculation.
     * @return The multiset Jaccard similarity of the two arguments.
     */

    public double calculate(String[] strings1, String[] strings2) {
        double jaccardSimilarity = 0;

        double intersections = 0;
        double union = 0;

        if (bagSemantics) {
            Object2IntOpenHashMap<String> strings1Map = new Object2IntOpenHashMap<>();
            Object2IntOpenHashMap<String> strings2Map = new Object2IntOpenHashMap<>();

            for (String token : strings1) {
                strings1Map.addTo(token, 1);
            }

            for (String token : strings2) {
                strings2Map.addTo(token, 1);
            }

            for (Object2IntOpenHashMap.Entry<String> entry : strings1Map.object2IntEntrySet()) {
                if (strings2Map.containsKey(entry.getKey())) {
                    intersections += Math.min(entry.getIntValue(), strings2Map.getInt(entry.getKey()));
                }
            }

            for (Object2IntOpenHashMap.Entry<String> entry : strings1Map.object2IntEntrySet()) {
                union += entry.getIntValue();
            }

            for (Object2IntOpenHashMap.Entry<String> entry : strings2Map.object2IntEntrySet()) {
                union += entry.getIntValue();
            }
        } else {
            Set<String> setStrings1 = new HashSet<>(Arrays.asList(strings1));
            Set<String> setStrings2 = new HashSet<>(Arrays.asList(strings2));

            intersections = Math.abs(setStrings1.stream().filter(setStrings2::contains).count());

            union = Math.abs ((double) setStrings1.size()) + Math.abs((double) setStrings2.size()) - intersections;
        }

        jaccardSimilarity = intersections / union;

        return jaccardSimilarity;
    }
}
