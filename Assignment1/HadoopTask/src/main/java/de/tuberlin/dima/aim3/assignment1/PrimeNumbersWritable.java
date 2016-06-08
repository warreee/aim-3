package de.tuberlin.dima.aim3.assignment1;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class PrimeNumbersWritable implements Writable {

    private int[] numbers;

    public PrimeNumbersWritable() {
        numbers = new int[0];
    }

    public PrimeNumbersWritable(int... numbers) {
        this.numbers = numbers;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numbers.length);
        Arrays.stream(numbers).forEach((v) -> writeToOut(out, v));
    }

    private void writeToOut(DataOutput out, int i) {
        try {
            out.writeInt(i);
        } catch (IOException ignored) {
        }
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();

        int[] temp = new int[length];
        for (int i = 0; i < length; i++) {
            temp[i] = in.readInt();
        }
        this.numbers = temp;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PrimeNumbersWritable) {
            PrimeNumbersWritable other = (PrimeNumbersWritable) obj;
            return Arrays.equals(numbers, other.numbers);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(numbers);
    }
}