package it.unipi.hadoop.hadoopobjects;

import com.google.gson.Gson;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Page implements WritableComparable<Page> {
    private String title;
    private double rank;


    public Page() { }

    public Page(final String title, final double rank) {
        set(title, rank);
    }

    public void setTitle(final String title) { this.title = title; }

    public void setRank(final double rank) { this.rank = rank; }

    public void set(final String title, final double rank) {
        setTitle(title);
        setRank(rank);
    }

    public void setByJson(final String json) {
        Page fromJson = new Gson().fromJson(json, Page.class);
        set(fromJson.getTitle(), fromJson.getRank());
    }

    public String getTitle() { return this.title; }

    public double getRank() { return this.rank; }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.title);
        out.writeDouble(this.rank);
    }

    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.rank = in.readDouble();
    }

    @Override
    public String toString() {
        String json = new Gson().toJson(this);
        return json;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Page){
            Page o = (Page)obj;
            return this.title.equals(o.getTitle()) && this.rank == o.getRank();
        }
        return false;
    }

    @Override
    public int hashCode() { return this.title.hashCode(); }

    public int compareTo(Page o) {
        double mis = (this.rank - o.getRank());
        if(mis > 0 ){
            return 1;
        } else if (mis < 0){
            return -1;
        }
        else{
            return this.getTitle().compareTo(o.getTitle());
        }
    }

}
