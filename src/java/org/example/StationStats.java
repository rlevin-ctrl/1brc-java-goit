package org.example;

public class StationStats {
    private int min;
    private int max;
    private long sum;
    private long count;

    public StationStats(int temp) {
        this.min = temp;
        this.max = temp;
        this.sum = temp;
        this.count = 1;
    }

    public StationStats(int min, int max, long sum, long count) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
    }

    public void add(int temp) {
        if (temp < min) min = temp;
        if (temp > max) max = temp;
        sum += temp;
        count++;
    }

    public void merge(StationStats other) {
        if (other.min < this.min) this.min = other.min;
        if (other.max > this.max) this.max = other.max;
        this.sum += other.sum;
        this.count += other.count;
    }

    public int getMin()   { return min; }
    public int getMax()   { return max; }
    public long getSum()  { return sum; }
    public long getCount(){ return count; }
}
