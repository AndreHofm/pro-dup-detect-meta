package de.uni_marburg.pdd.metadata.data_profiling;

import de.uni_marburg.pdd_metadata.data_profiling.AttributeWeightProfiler;
import de.uni_marburg.pdd_metadata.data_profiling.structures.AttributeWeight;
import de.uni_marburg.pdd_metadata.duplicate_detection.ResultCollector;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;

class AttributeWeightProfilerTest {

    private Configuration config;

    private AttributeWeightProfiler attributeWeightProfiler;

    @BeforeEach
    void setUp() {
        config = new Configuration();
        config.setMaxFDDeterminant(3);
        config.setNullThreshold(0.05);
        config.setAttributeSeparator(';');
        config.setHasHeadline(true);
        config.setCharset(StandardCharsets.ISO_8859_1);
        config.setFILTER_WITH_MISSING_INFO(false);
        config.setFILTER_WITH_FD_INFO(false);
        config.setFILTER_WITH_PK(false);
        config.setFILTER_WITH_IND_INFO(false);
        config.setUSE_WEIGHTS(false);
        config.setUSE_FD_INFO(false);
        config.setUSE_UCC_INFO(false);
    }

    @Test
    void testMissingValuesFiltering() throws Exception {
        String fileName = "test_file_missing_values";
        config.setFILTER_WITH_MISSING_INFO(true);

        setUpForEachTest(fileName);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight = new AttributeWeight(0, "Attribute1");

        Assertions.assertEquals(1, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight, attributeWeightProfiler.getAttributeWeights().get(0));
    }

    @Test
    void testFDFiltering() throws Exception {
        String fileName = "test_file_fd_filter";

        setUpForEachTest(fileName);

        config.setFILTER_WITH_FD_INFO(true);
        config.setUSE_FD_INFO(true);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight1 = new AttributeWeight(0, "Attribute1");

        Assertions.assertEquals(1, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight1, attributeWeightProfiler.getAttributeWeights().get(0));
    }

    @Test
    void testPKFiltering() throws Exception {
        String fileName = "test_file_pk_filter";

        setUpForEachTest(fileName);

        config.setFILTER_WITH_PK(true);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight = new AttributeWeight(1, "Attribute2");

        Assertions.assertEquals(1, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight, attributeWeightProfiler.getAttributeWeights().get(0));
    }

    @Test
    void testINDFiltering() throws Exception {
        String fileName = "test_file_ind_filter";

        setUpForEachTest(fileName);

        config.setFILTER_WITH_IND_INFO(true);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight1 = new AttributeWeight(0, "Attribute1");
        AttributeWeight attributeWeight2 = new AttributeWeight(2, "Attribute3");

        Assertions.assertEquals(2, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight1, attributeWeightProfiler.getAttributeWeights().get(0));
        Assertions.assertEquals(attributeWeight2, attributeWeightProfiler.getAttributeWeights().get(1));
    }

    @Test
    void testFDWeighting() throws Exception {
        String fileName = "test_file_fd_filter";
        config.setFILTER_WITH_MISSING_INFO(true);

        setUpForEachTest(fileName);

        config.setUSE_WEIGHTS(true);
        config.setUSE_FD_INFO(true);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight1 = new AttributeWeight(0, "Attribute1");
        AttributeWeight attributeWeight2 = new AttributeWeight(1, "Attribute2");
        attributeWeight1.setWeight(1.0);
        attributeWeight2.setWeight(0);

        Assertions.assertEquals(2, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight1, attributeWeightProfiler.getAttributeWeights().get(0));
        Assertions.assertEquals(attributeWeight2, attributeWeightProfiler.getAttributeWeights().get(1));
    }

    @Test
    void testUCCWeighting() throws Exception {
        String fileName = "test_file_ucc_weight";
        config.setFILTER_WITH_MISSING_INFO(true);

        setUpForEachTest(fileName);

        config.setUSE_WEIGHTS(true);
        config.setUSE_UCC_INFO(true);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight1 = new AttributeWeight(0, "Attribute1");
        AttributeWeight attributeWeight2 = new AttributeWeight(1, "Attribute2");
        AttributeWeight attributeWeight3 = new AttributeWeight(2, "Attribute3");
        attributeWeight1.setWeight(0.5);
        attributeWeight2.setWeight(0);
        attributeWeight3.setWeight(0.5);

        Assertions.assertEquals(3, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight1, attributeWeightProfiler.getAttributeWeights().get(0));
        Assertions.assertEquals(attributeWeight2, attributeWeightProfiler.getAttributeWeights().get(2));
        Assertions.assertEquals(attributeWeight3, attributeWeightProfiler.getAttributeWeights().get(1));
    }

    @Test
    void testFDAndUCCWeighting() throws Exception {
        String fileName = "test_file_fd_ucc_weight";
        config.setFILTER_WITH_MISSING_INFO(true);

        setUpForEachTest(fileName);

        config.setUSE_WEIGHTS(true);
        config.setUSE_FD_INFO(true);
        config.setUSE_UCC_INFO(true);

        attributeWeightProfiler.execute();

        AttributeWeight attributeWeight1 = new AttributeWeight(0, "Attribute1");
        AttributeWeight attributeWeight2 = new AttributeWeight(1, "Attribute2");
        AttributeWeight attributeWeight3 = new AttributeWeight(2, "Attribute3");
        AttributeWeight attributeWeight4 = new AttributeWeight(3, "Attribute4");
        attributeWeight1.setWeight((double) 2 / 3);
        attributeWeight2.setWeight(0);
        attributeWeight3.setWeight((double) 1 / 3);
        attributeWeight4.setWeight(0);

        Assertions.assertEquals(4, attributeWeightProfiler.getAttributeWeights().size());
        Assertions.assertEquals(attributeWeight1, attributeWeightProfiler.getAttributeWeights().get(0));
        Assertions.assertEquals(attributeWeight2, attributeWeightProfiler.getAttributeWeights().get(2));
        Assertions.assertEquals(attributeWeight3, attributeWeightProfiler.getAttributeWeights().get(1));
        Assertions.assertEquals(attributeWeight4, attributeWeightProfiler.getAttributeWeights().get(3));
    }

    void setUpForEachTest(String fileName) throws Exception {
        config.setFileName(fileName + ".csv");
        config.setDatasetName(fileName);
        URL resource = getClass().getClassLoader().getResource(fileName + ".csv");
        assert resource != null;
        File file = new File(resource.toURI());

        DataReader dataReader = new DataReader(file.getAbsolutePath(), config);
        ResultCollector resultCollector = new ResultCollector(dataReader, config);
        attributeWeightProfiler = new AttributeWeightProfiler(dataReader, file.getAbsolutePath(), config, resultCollector);
    }
}

