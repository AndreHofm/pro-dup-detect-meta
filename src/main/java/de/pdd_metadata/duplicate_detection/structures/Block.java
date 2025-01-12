package de.pdd_metadata.duplicate_detection.structures;

import de.pdd_metadata.duplicate_detection.structures.multi_block.RecordPair;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.*;

@AllArgsConstructor
@NoArgsConstructor
public class Block {
    public int blockId;
    public int size;
    public List<Record> recordsA = new ArrayList<>();
    public List<Record> recordsB = new ArrayList<>();
    public HashMap<Integer, Record> records = new HashMap<>();
    public double score;
    public int evaluations;
    public int noMatchingPairs;

    public Block(int blockId, int size, HashMap<Integer, Record> records) {
        this.blockId = blockId;
        this.size = size;
        this.records = records;
    }

    public void calcScore() {
        this.score = noMatchingPairs * 1.0 / evaluations;
    }

    public void add(Record r) {
        if (r.typeSet == 'A')
            recordsA.add(r);
        else
            recordsB.add(r);
        size++;
    }

    public Set<RecordPair> sample(double ratio) {
        Random r = new Random();
        Set<RecordPair> set = new HashSet<>();
        size = recordsA.size() + recordsB.size();
        int no_sample = (int) Math.round(size / ratio);
        for (int i = 0; i < no_sample; i++) {
            int a = r.nextInt(recordsA.size());
            int b = r.nextInt(recordsB.size());
            Record rA = recordsA.get(a);
            Record rB = recordsB.get(b);
            RecordPair rp = new RecordPair(rA, rB);
            set.add(rp);
        }
        return set;
    }
}
