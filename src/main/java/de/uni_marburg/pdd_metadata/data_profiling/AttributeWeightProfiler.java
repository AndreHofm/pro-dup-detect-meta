package de.uni_marburg.pdd_metadata.data_profiling;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeWeight;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
public class AttributeWeightProfiler {
    private FDProfiler fdProfiler;
    private INDProfiler indProfiler;
    private UCCProfiler uccProfiler;
    private DataReader dataReader;
    private List<AttributeWeight> attributeWeights;
    private String datasetName;
    private Configuration config;
    private Logger log = LogManager.getLogger(AttributeWeightProfiler.class);

    public AttributeWeightProfiler(DataReader dataReader, String input, Configuration config) throws FileNotFoundException {
        this.dataReader = dataReader;

        DefaultFileInputGenerator fileInputGenerator = getInputGenerator(input);

        this.uccProfiler = new UCCProfiler(fileInputGenerator, config);
        this.fdProfiler = new FDProfiler(fileInputGenerator, dataReader, config);
        this.indProfiler = new INDProfiler(fileInputGenerator, config);
        this.attributeWeights = new ArrayList<>();
        this.datasetName = config.getDatasetName();
        this.config = config;
    }

    public void execute() throws Exception {
        this.log.info("Starting Attribute Weight Profiler...");
        var starTime = System.currentTimeMillis();
        initializeAttributeScoreList();

        Set<String> filterAttributesByNullValues = new HashSet<>();

        Map<String, Long> sortedFilteredFDs = new HashMap<>();

        Map<String, Long> sortedFilteredUCCs = new HashMap<>();

        Set<String> primaryKeys = new HashSet<>();

        Set<String> dependantINDAttributes = new HashSet<>();

        if (config.isFILTER_WITH_MISSING_INFO()) {
            filterAttributesByNullValues.addAll(filterAttributesByNullValues());

            this.attributeWeights.removeIf(attributeWeight -> !filterAttributesByNullValues.contains(attributeWeight.getAttribute()));
        }

        if (config.isFILTER_WITH_IND_INFO()) {
            indProfiler.executePartialINDProfiler();
            indProfiler.executeFullINDProfiler();
            Set<InclusionDependency> simINDs = indProfiler.getSimINDs();
            Set<InclusionDependency> fullINDS = removeIdenticalINDs(indProfiler.getFullINDs());
            simINDs.addAll(fullINDS);
            dependantINDAttributes.addAll(getDependantINDAttributes(simINDs));

            this.log.info("Number of INDs: {}", simINDs.size());
            this.log.info("INDs: {}", dependantINDAttributes);

            this.attributeWeights.removeIf(attributeWeight -> dependantINDAttributes.contains(attributeWeight.getAttribute()));
        }

        if (config.isFILTER_WITH_PK()) {
            uccProfiler.executeKeyProfiler();
            Set<UniqueColumnCombination> keys = uccProfiler.getKeys();
            primaryKeys.addAll(getPKs(keys));

            this.log.info("Keys: {}", primaryKeys);
            this.attributeWeights.removeIf(attributeWeight -> primaryKeys.contains(attributeWeight.getAttribute()));
        }

        if (config.isUSE_FD_INFO()) {
            fdProfiler.executeFullFDProfiler();
            List<FunctionalDependency> fullFDs = fdProfiler.getFullFDs();

            Map<String, Long> filteredFDs = filteringFDs(fullFDs, filterAttributesByNullValues, primaryKeys, dependantINDAttributes);
            sortedFilteredFDs = sortDependencyMap(filteredFDs);

            this.log.info("Number of FDs: {}", fullFDs.size());
            this.log.info("FDs: {}", sortedFilteredFDs);

            if (config.isFILTER_WITH_FD_INFO()) {
                this.attributeWeights.removeIf(attributeWeight -> (!new HashSet<>(filteredFDs.keySet()).contains(attributeWeight.getAttribute())));
            }
        }

        if (config.isUSE_UCC_INFO()) {
            uccProfiler.executeFullUCCProfiler();
            Set<UniqueColumnCombination> fullUCCs = uccProfiler.getFullUCCs();

            Map<String, Long> filteredUCCs = filteringUCCs(fullUCCs, filterAttributesByNullValues, primaryKeys, dependantINDAttributes);
            sortedFilteredUCCs = sortDependencyMap(filteredUCCs);

            this.log.info("Number of UCCs: {}", fullUCCs.size());
            //this.log.info("UCCs: {}", sortedFilteredUCCs);
        }

        if (config.isUSE_WEIGHTS()) {
            this.weightAttributes(sortedFilteredFDs, sortedFilteredUCCs);
        }

        var endTime = System.currentTimeMillis() - starTime;
        this.log.info("Number of Attributes: {}", this.attributeWeights.size());
        this.log.info("Ending Attribute Weight Profiler - (Runtime: {}ms)", endTime);
    }

    private void weightAttributes(Map<String, Long> fds, Map<String, Long> uccs) {
        List<String> sortedFDAttributes = fds.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .toList();

        List<String> sortedUCCAttributes = uccs.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .toList();

        int sumFD = 0;
        for (Long value : fds.values()) {
            sumFD = sumFD + value.intValue();
        }

        int sumUCC = 0;
        for (Long value : uccs.values()) {
            sumUCC = sumUCC + value.intValue();
        }

        int sum = sumFD + sumUCC;

        HashMap<String, Double> nameWeightFD = calculateAttributeDependencyWeight(fds, sortedFDAttributes, sum);

        HashMap<String, Double> nameWeightUCC = calculateAttributeDependencyWeight(uccs, sortedUCCAttributes, sum);

        for (AttributeWeight attributeWeight : this.attributeWeights) {
            String attribute = attributeWeight.getAttribute();
            double weight = 0;
            if (nameWeightFD.containsKey(attribute) && nameWeightUCC.containsKey(attribute)) {
                weight = nameWeightFD.get(attribute) + nameWeightUCC.get(attribute);
            } else if (nameWeightFD.containsKey(attribute) && !nameWeightUCC.containsKey(attribute)) {
                weight = nameWeightFD.get(attribute) / (nameWeightUCC.isEmpty() ? 1.0 : 2.0);
            } else if (!nameWeightFD.containsKey(attribute) && nameWeightUCC.containsKey(attribute)) {
                weight = nameWeightUCC.get(attribute) / (nameWeightFD.isEmpty() ? 1.0 : 2.0);
            }

            attributeWeight.setWeight(weight);
        }
    }

    private HashMap<String, Double> calculateAttributeDependencyWeight(Map<String, Long> dependencies, List<String> sortedFDAttributes, int sumDependency) {
        HashMap<String, Double> nameWeightDependency = new HashMap<>();
        for (int i = 0; i < dependencies.size(); i++) {
            String name = sortedFDAttributes.get(i);
            double weight = (double) dependencies.get(name) / sumDependency;
            nameWeightDependency.put(name, weight);
        }

        return nameWeightDependency;
    }

    private Set<String> filterAttributesByNullValues() {
        int datasetSize = this.dataReader.getNumRecords();
        HashMap<String, Integer> attributesNull = this.dataReader.countNullValues();

        return attributesNull.entrySet().stream()
                .filter(x -> !(((double) x.getValue() / datasetSize) >= config.getNullThreshold()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    private Map<String, Long> filteringFDs(List<FunctionalDependency> fds, Set<String> filterAttributesByNullValues, Set<String> primaryKeys, Set<String> dependantINDAttributes) {
        Stream<String> fdStream = fds.stream()
                .flatMap(fd -> fd.getDeterminant().getColumnIdentifiers().stream())
                .map(ColumnIdentifier::getColumnIdentifier);

        return filterDependencies(filterAttributesByNullValues, primaryKeys, dependantINDAttributes, fdStream);
    }

    private Set<String> getPKs(Set<UniqueColumnCombination> uccs) {
        return uccs.stream()
                .flatMap(x -> x.getColumnCombination().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .collect(Collectors.toSet());
    }

    private Map<String, Long> filteringUCCs(Set<UniqueColumnCombination> uccs, Set<String> filterAttributesByNullValues, Set<String> primaryKeys, Set<String> dependantINDAttributes) {
        Stream<String> uccStream = uccs.stream()
                .flatMap(x -> x.getColumnCombination().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""));

        return filterDependencies(filterAttributesByNullValues, primaryKeys, dependantINDAttributes, uccStream);
    }

    private Map<String, Long> filterDependencies(Set<String> filterAttributesByNullValues, Set<String> primaryKeys, Set<String> dependantINDAttributes, Stream<String> uccStream) {
        if (config.isFILTER_WITH_MISSING_INFO()) {
            uccStream = uccStream.filter(filterAttributesByNullValues::contains);
        }

        if (config.isFILTER_WITH_PK()) {
            uccStream = uccStream.filter(column -> !primaryKeys.contains(column));
        }

        if (config.isFILTER_WITH_IND_INFO()) {
            uccStream = uccStream.filter(column -> !dependantINDAttributes.contains(column));
        }

        return uccStream.collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    }

    private Set<String> getDependantINDAttributes(Set<InclusionDependency> inds) {
        return inds.stream()
                .flatMap(x -> x.getDependant().getColumnIdentifiers().stream())
                .map(x -> x.toString().replace(datasetName + ".csv.", ""))
                .collect(Collectors.toSet());
    }

    private Map<String, Long> getDependantFDAttributes(List<FunctionalDependency> fds, Set<String> filterAttributesByNullValues, Set<String> filteredUCCs, Set<String> filteredINDs) {
        return fds.stream()
                .map(fd -> fd.getDependant().getColumnIdentifier())
                .filter(filterAttributesByNullValues::contains)
                .filter(column -> !filteredINDs.contains(column) && !filteredUCCs.contains(column))
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));
    }

    private Set<InclusionDependency> removeIdenticalINDs(Set<InclusionDependency> inds) {
        List<InclusionDependency> list = new ArrayList<>(inds.stream().toList());

        for (int i = 0; i < list.size(); i++) {
            for (int j = 0; j < list.size(); j++) {
                if (list.get(i).getDependant().equals(list.get(j).getReferenced()) && list.get(j).getDependant().equals(list.get(i).getReferenced())) {
                    list.remove(list.get(j));
                }
            }
        }

        return new HashSet<>(list);
    }

    private Map<String, Long> sortDependencyMap(Map<String, Long> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .collect(LinkedHashMap::new,
                        (m, e) -> m.put(e.getKey(), e.getValue()),
                        LinkedHashMap::putAll);
    }

    private void initializeAttributeScoreList() {
        String[] attributeNames = dataReader.getAttributeNames();

        for (int i = 0; i < attributeNames.length; i++) {
            AttributeWeight attribute = new AttributeWeight(i, attributeNames[i]);
            this.attributeWeights.add(attribute);
        }
    }

    private static DefaultFileInputGenerator getInputGenerator(String path) {
        try {
            return new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
                    path,
                    true,
                    ';',
                    '\"',
                    '\\',
                    false,
                    true,
                    0,
                    true,
                    true,
                    ""
            ));
        } catch (AlgorithmConfigurationException e) {
            throw new RuntimeException(e);
        }
    }
}
