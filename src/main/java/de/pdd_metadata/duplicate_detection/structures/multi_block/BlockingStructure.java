/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pdd_metadata.duplicate_detection.structures.multi_block;

import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import de.pdd_metadata.duplicate_detection.structures.Block;
import de.pdd_metadata.duplicate_detection.structures.Record;

/**
 * @author karap
 */
public class BlockingStructure {

    public HashMap<Integer, Block> blocks = new HashMap<>();

    public PriorityQueue<Block> pq = new PriorityQueue<>(10, new ScoreComparator());

    public void init() {
        for (int i = 0; i < blocks.size(); i++) {
            pq.add(blocks.get(i));
        }
    }

    static class ScoreComparator implements Comparator<Block> {

        // Overriding compare()method of Comparator
        // for descending order of cgpa
        public int compare(Block b1, Block b2) {
            if (b1.score < b2.score) {
                return 1;
            } else if (b1.score > b2.score) {
                return -1;
            }
            return 0;
        }
    }

    public void populate(Record r) {
        if (blocks.containsKey(r.blockingKey)) {
            Block b = blocks.get(r.blockingKey);
            b.add(r);
        } else {
            Block b = new Block();
            b.add(r);
            blocks.put(r.blockingKey, b);
        }
    }

    public int calcS(double delta, double ratio) {
        int s = (int) Math.round(Math.log(1 / delta) * ratio);
        return s;
    }

}
