/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pdd_metadata.duplicate_detection.structures.multi_block;

import de.pdd_metadata.duplicate_detection.structures.Record;

/**
 * @author karap
 */
public class RecordPair {
    public Record rA;
    public Record rB;
    public int IdA;
    public int IdB;

    public RecordPair(Record rA, Record rB) {
        this.rA = rA;
        this.rB = rB;
        this.IdA = rA.id;
        this.IdB = rB.id;
    }
}
