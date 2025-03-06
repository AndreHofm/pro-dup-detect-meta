package de.uni_marburg.pdd_metadata.data_profiling;

import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.uni_marburg.pdd_metadata.data_profiling.structures.PdepTuple;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AllArgsConstructor
public class MetaUtils {

    public static PdepTuple getPdep(FunctionalDependency fd, Map<String, List<String>> columnValues) {
        Set<ColumnIdentifier> determinant = fd.getDeterminant().getColumnIdentifiers();
        ColumnIdentifier dependant = fd.getDependant();

        List<String> values = columnValues.get(dependant.getColumnIdentifier());

        int N = values.size();

        Map<String, Integer> frequencyMapDep = createFrequencyMap(values);
        Map<String, Integer> frequencyMapDet = createFrequencyMap(determinant, N, columnValues);

        double pdep = 1.0;
        double gpdep = gpdep(frequencyMapDet, frequencyMapDep, N);
        return new PdepTuple(pdep, gpdep);
    }


    public static double epdep(int dA, Map<String, Integer> valuesB, int N) {
        double pdepB = pdep(valuesB, N);

        return pdepB + (dA - 1.0) / (N - 1.0) * (1.0 - pdepB);
    }

    private static double pdep(Map<String, Integer> valuesB, int N) {
        double result = 0;
        for (Integer count : valuesB.values()) {
            result += (count * count);
        }
        return result / (N * N);
    }

    public static double gpdep(Map<String, Integer> valuesA, Map<String, Integer> valuesB, int N) {
        double pdepAB = 1;
        double epdepAB = epdep(valuesA.size(), valuesB, N);

        return pdepAB - epdepAB;
    }

    public static Map<String, Integer> createFrequencyMap(List<String> values) {
        Map<String, Integer> frequencyMap = new HashMap<>();

        for (String value : values) {
            frequencyMap.put(value, frequencyMap.getOrDefault(value, 0) + 1);
        }

        return frequencyMap;
    }

    public static Map<String, Integer> createFrequencyMap(Set<ColumnIdentifier> columns, int size, Map<String, List<String>> columnValues) {
        Map<String, Integer> frequencyMap = new HashMap<>();

        for (int i = 0; i < size; i++) {
            StringBuilder concatenatedValue = new StringBuilder();
            for (ColumnIdentifier col : columns) {
                concatenatedValue.append(columnValues.get(col.getColumnIdentifier()).get(i));
            }
            String key = concatenatedValue.toString();
            frequencyMap.put(key, frequencyMap.getOrDefault(key, 0) + 1);
        }

        return frequencyMap;
    }
}
