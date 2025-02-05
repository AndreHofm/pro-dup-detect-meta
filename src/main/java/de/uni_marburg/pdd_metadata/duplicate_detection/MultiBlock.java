package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Block;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.multi_block.BlockingStructure;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Record;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.multi_block.RecordPair;
import de.uni_marburg.pdd_metadata.similarity_measures.Levenshtein;

import java.util.*;

public class MultiBlock {
    public Levenshtein levenshtein = new Levenshtein(200);
    public Set<Duplicate> duplicates = new HashSet<>();

    public void execute(BlockingStructure H) {
        // BlockingStructure H = new BlockingStructure();
        double ratio = 33.0;
        double delta = 0.1;
        int s = H.calcS(delta, ratio);
        H.init(); // init Priority Queue
        HashSet<RecordPair> matchingPairs = new HashSet<>();

        for (int i = 1; i <= s; i++) {
            for (Block block : H.pq) {
                Set<RecordPair> set = block.sample(ratio);
                for (RecordPair rp : set) {
                    Record rA =  rp.rA;
                    Record rB =  rp.rB;
                    block.evaluations++;

                    // Duplicate duplicate = new Duplicate(rA.id, rB.id);

                    if (levenshtein.calculateSimilarityOf(rA.values, rB.values) >= 0.7) {
                        matchingPairs.add(rp);
                        block.noMatchingPairs++;
                        //duplicates.add(duplicate);
                    }
                }
            }
        }
    }

    public void execute2(BlockingStructure b) {
        double ratio = 33.0;
        double delta = 0.1;
        int s = b.calcS(delta, ratio);

        Set<RecordPair> evaluated = new HashSet<>();
        b.init();
        PriorityQueue<Block> pq = b.pq;

        for (int i = 1; i <= s; i++) {
            for (Block block : pq) {
                Set<RecordPair> recordPairs = block.sample(ratio);

                for (RecordPair recordPair : recordPairs) {
                    if (!evaluated.contains(recordPair)) {
                        evaluated.add(recordPair);

                        Record recordA = recordPair.rA;
                        Record recordB = recordPair.rB;

                        if (levenshtein.calculateSimilarityOf(recordA.values, recordB.values) >= 0.7) {
                            block.noMatchingPairs++;
                            //duplicates.add(new Duplicate(recordA.id, recordB.id));
                        }

                        block.calcScore();
                    }
                }
            }
        }
    }
}
