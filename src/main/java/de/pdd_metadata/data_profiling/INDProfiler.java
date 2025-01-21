package de.pdd_metadata.data_profiling;

import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Sandy;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.util.PartialIND;
//import de.hpi.mpss2015n.approxind.FAIDA;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.result_receiver.ResultCache;
import lombok.Getter;

import java.io.FileNotFoundException;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class INDProfiler {
    private Sindy sindy;
    private Sandy sandy;
    // private FAIDA faida;
    private Set<InclusionDependency> inds = new HashSet<>();
    private Set<PartialIND> partialINDS = new HashSet<>();
    private FileInputGenerator fileInputGenerator;

    public INDProfiler(FileInputGenerator fileInputGenerator) {
        this.fileInputGenerator = fileInputGenerator;
        // this.faida = new FAIDA();
    }

    static class Parameters {
        private static final boolean NULL_EQUALS_NULL = true;
        private static final boolean VALIDATE_PARALLEL = true;
        private static final boolean ENABLE_MEMORY_GUARDIAN = true;
        private static final boolean DETECT_NARY = true;
        private static final int MAX_SEARCH_SPACE_LEVEL = -1;
        private static final int FILE_MAX_ROWS = -1;
    }

    ;

    public void executePartialINDProfiler() {
        sandy.setDropNulls(false);
        sandy.setFieldSeparator('\t');
        sandy.setMaxArity(1);
        sandy.setMinRelativeOverlap(0.0);
        sandy.run();
    }
/*
    public void executeFullINDProfiler() throws AlgorithmExecutionException, FileNotFoundException {
        ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(fileInputGenerator));

        faida.setRelationalInputConfigurationValue(FAIDA.Identifier.INPUT_FILES.name(), fileInputGenerator);
        faida.setResultReceiver(resultReceiver);
        faida.execute();

        List<Result> results = resultReceiver.fetchNewResults();

        inds = results.stream().map(x -> (InclusionDependency) x).collect(Collectors.toSet());
    }

 */

    private static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();

        return relationalInput.columnNames().stream()
                .map(columnName -> new ColumnIdentifier(tableName, columnName))
                .toList();
    }
}
