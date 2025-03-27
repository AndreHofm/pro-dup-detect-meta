package de.uni_marburg.pdd_metadata.duplicate_detection.structures;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.*;

@AllArgsConstructor
@NoArgsConstructor
public class Block {
    public int blockId;
    public int size;
    public HashMap<Integer, Record> records = new HashMap<>();
}
