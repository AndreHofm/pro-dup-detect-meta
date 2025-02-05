package de.uni_marburg.pdd_metadata.data_profiling;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import org.apache.commons.io.output.NullOutputStream;

import java.io.PrintStream;
import java.util.List;

public class DependencyProfiler {
    protected static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator relationalInputGenerator) throws InputGenerationException, AlgorithmConfigurationException {
        RelationalInput relationalInput = relationalInputGenerator.generateNewCopy();
        String tableName = relationalInput.relationName();

        return relationalInput.columnNames().stream()
                .map(columnName -> new ColumnIdentifier(tableName, columnName))
                .toList();
    }

    protected static void suppressSysOut(Runnable method) throws RuntimeException {
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(new NullOutputStream()));
        method.run();
        System.setOut(originalOut);
    }
}
