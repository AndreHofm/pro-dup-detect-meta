package de.uni_marburg.pdd_metadata.duplicate_detection.structures;

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

    public Block(int blockId, int size, HashMap<Integer, Record> records) {
        this.blockId = blockId;
        this.size = size;
        this.records = records;
    }
}
