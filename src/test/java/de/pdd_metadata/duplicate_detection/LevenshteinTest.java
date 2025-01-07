package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.similarity_measures.Levenshtein;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LevenshteinTest {

    @Test
    public void testCorrectnessStrings() {
        Levenshtein levenshtein = null;
        double result = 0;

        levenshtein = new Levenshtein();
        result = levenshtein.calculate("Big Data", "Big Data");
        assertEquals(1, result, 0.000001);

        levenshtein = new Levenshtein();
        result = levenshtein.calculate("Big Data Systems", "Data Integration");
        assertEquals(1 - (double) 14 / 16, result, 0.000001);

        levenshtein = new Levenshtein();
        result = levenshtein.calculate("Database Systems", "VL Datenbanksysteme");
        assertEquals(1 - (double) 10 / 19, result, 0.000001);

        levenshtein = new Levenshtein();
        result = levenshtein.calculate("Integrations", "Itnegratoins");
        assertEquals(1 - (double) 4 / 12, result, 0.000001);
    }

    @Test
    public void testCorrectnessStringLists() {
        Levenshtein levenshtein = null;
        double result = 0;

        levenshtein = new Levenshtein();
        result = levenshtein.calculate(new String[]{"a", "b", "a", "b", "c", "c", "d"}, new String[]{"a", "b", "a", "b", "c", "f", "e"});
        assertEquals(1 - (double) 2 / 7, result, 0.000001);

        levenshtein = new Levenshtein();
        result = levenshtein.calculate(new String[]{"a", "b", "a", "b", "c", "c", "d"}, new String[]{"a", "b", "a", "b", "c", "f", "e"});
        assertEquals(1 - (double) 2 / 7, result, 0.000001);
    }

    @Test
    public void testNull() {
        Levenshtein levenshtein = null;
        double result = 0;

        levenshtein = new Levenshtein();
        result = levenshtein.calculate("Integrations", "");
        assertEquals(0, result, 0.000001);

        levenshtein = new Levenshtein();
        result = levenshtein.calculate(new String[]{"a", "", "a", "b", "c", "c", ""}, new String[]{"a", "", "a", "b", "c", "f", "e"});
        assertEquals(1 - (double) 2 / 7, result, 0.000001);
    }
}
