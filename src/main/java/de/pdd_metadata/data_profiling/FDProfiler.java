package de.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.algorithms.Pyro;
import de.hpi.isg.pyro.model.PartialFD;
import de.hpi.isg.pyro.model.Vertical;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.hyfd.HyFD;
import de.metanome.backend.result_receiver.ResultCache;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class FDProfiler {
    private final Pyro pyro = new Pyro();
    private final HyFD hyFD = new HyFD();
    private FileInputGenerator fileInputGenerator;
    private List<PartialFD> partialFDs = new ArrayList<>();
    private Set<FunctionalDependency> fullFDs = new HashSet<>();

    public FDProfiler(FileInputGenerator fileInputGenerator) {
        this.fileInputGenerator = fileInputGenerator;
    }

    public HashMap<Vertical, Long> executePartialFDProfiler() throws Exception {
        pyro.setRelationalInputConfigurationValue("inputFile", fileInputGenerator);
        pyro.setBooleanConfigurationValue("isNullEqualNull", true);
        pyro.setBooleanConfigurationValue("isFindKeys", false);

        pyro.setFdConsumer(partialFDs::add);

        pyro.execute();

        return (HashMap<Vertical, Long>) partialFDs.stream().collect(Collectors.groupingBy(key -> key.lhs, Collectors.counting()));
    }

    public void executeFullFDProfiler() throws Exception {
        hyFD.setRelationalInputConfigurationValue("INPUT_GENERATOR", fileInputGenerator);

        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));

        hyFD.setResultReceiver(resultReceiver);

        hyFD.execute();

        List<Result> results = resultReceiver.fetchNewResults();

        fullFDs = results.stream().map(x -> (FunctionalDependency) x).collect(Collectors.toSet());
    }

    private static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();

        return relationalInput.columnNames().stream()
                .map(columnName -> new ColumnIdentifier(tableName, columnName))
                .toList();
    }
}
