package de.pdd_metadata.duplicate_detection.structures;

import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.List;

@AllArgsConstructor
public class Block {
    public int blockId;
    public int size;
    public List<Record> recordA;
    public List<Record> recordB;
    public HashMap<Integer, Record> records;
}
