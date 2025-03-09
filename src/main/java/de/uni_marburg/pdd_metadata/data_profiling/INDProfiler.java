package de.uni_marburg.pdd_metadata.data_profiling;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.binder.BINDERFile;
import de.metanome.algorithms.sawfish.SawfishInterface;
import de.metanome.backend.algorithm_execution.TempFileGenerator;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class INDProfiler extends DependencyProfiler {
    private BINDERFile binder;
    private Set<InclusionDependency> inds = new HashSet<>();
    private Set<InclusionDependency> partialINDS = new HashSet<>();
    private DefaultFileInputGenerator fileInputGenerator;
    private SawfishInterface sawfish;
    private RelationalInputGenerator[] inputs;
    private Logger log = LogManager.getLogger(INDProfiler.class);
    private Configuration config;

    public INDProfiler(DefaultFileInputGenerator fileInputGenerator, Configuration config) {
        this.fileInputGenerator = fileInputGenerator;
        this.binder = new BINDERFile();
        this.sawfish = new SawfishInterface();
        this.inputs = new RelationalInputGenerator[]{fileInputGenerator, fileInputGenerator};
        this.config = config;
    }

    static class Parameters {
        private static final boolean DETECT_NARY = false;
        private static final int MAX_SEARCH_SPACE_LEVEL = -1;
        private static final int FILE_MAX_ROWS = -1;
    }

    public void executePartialINDProfiler() throws Exception {
        ResultCache resultReceiver = this.getResultReceiver(inputs);

        sawfish.setRelationalInputConfigurationValue(SawfishInterface.Identifier.INPUT_FILES.name(), inputs);
        sawfish.setStringConfigurationValue(SawfishInterface.Identifier.similarityThreshold.name(), String.valueOf(config.getIndThreshold()));
        sawfish.setBooleanConfigurationValue(SawfishInterface.Identifier.ignoreShortStrings.name(), false);
        sawfish.setBooleanConfigurationValue(SawfishInterface.Identifier.measureTime.name(), false);
        sawfish.setBooleanConfigurationValue(SawfishInterface.Identifier.ignoreNumericColumns.name(), false);
        sawfish.setResultReceiver(resultReceiver);
        sawfish.setTempFileGenerator(new TempFileGenerator());

        suppressSysOut(() -> {
            try {
                sawfish.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Result> results = resultReceiver.fetchNewResults();
        partialINDS = results.stream().map(x -> (InclusionDependency) x)
                .filter(x -> !x.getDependant().toString().equals(x.getReferenced().toString()))
                .collect(Collectors.toSet());
    }

    public void executeFullINDProfiler() throws AlgorithmExecutionException, FileNotFoundException {
        ResultCache resultReceiver = this.getResultReceiver(inputs);

        binder.setRelationalInputConfigurationValue(BINDERFile.Identifier.INPUT_FILES.name(), inputs);
        binder.setIntegerConfigurationValue(BINDERFile.Identifier.MAX_NARY_LEVEL.name(), Parameters.MAX_SEARCH_SPACE_LEVEL);
        binder.setIntegerConfigurationValue(BINDERFile.Identifier.INPUT_ROW_LIMIT.name(), Parameters.FILE_MAX_ROWS);
        binder.setBooleanConfigurationValue(BINDERFile.Identifier.DETECT_NARY.name(), Parameters.DETECT_NARY);
        binder.setResultReceiver(resultReceiver);

        suppressSysOut(() -> {
            try {
                binder.execute();
            } catch (AlgorithmExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        List<Result> results = resultReceiver.fetchNewResults();
        inds = results.stream().map(x -> (InclusionDependency) x)
                .filter(x -> !x.getDependant().toString().equals(x.getReferenced().toString()))
                .collect(Collectors.toSet());
    }

    private ResultCache getResultReceiver(RelationalInputGenerator[] inputs) throws InputGenerationException, AlgorithmConfigurationException, FileNotFoundException {
        List<ColumnIdentifier> columnIdentifiers = new ArrayList<>();
        columnIdentifiers.addAll(getAcceptedColumns(inputs[0]));
        columnIdentifiers.addAll(getAcceptedColumns(inputs[1]));

        return new ResultCache("MetanomeMock", columnIdentifiers);
    }
}
