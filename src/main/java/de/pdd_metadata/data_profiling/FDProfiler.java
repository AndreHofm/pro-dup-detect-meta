package de.pdd_metadata.data_profiling;

import de.hpi.isg.pyro.algorithms.Pyro;
import de.hpi.isg.pyro.model.PartialFD;
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
import de.pdd_metadata.io.DataReader;
import lombok.Getter;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class FDProfiler {
    private final Pyro pyro = new Pyro();
    private final HyFD hyFD = new HyFD();
    private FileInputGenerator fileInputGenerator;
    private DataReader dataReader;
    private Set<PartialFD> partialFDs = new HashSet<>();
    private Set<FunctionalDependency> fullFDs = new HashSet<>();

    public void executePartialFDProfiler() throws Exception {
        pyro.setRelationalInputConfigurationValue("inputFile", fileInputGenerator);
        pyro.setBooleanConfigurationValue("isNullEqualNull", true);

        pyro.setFdConsumer(partialFDs::add);

        pyro.execute();
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
