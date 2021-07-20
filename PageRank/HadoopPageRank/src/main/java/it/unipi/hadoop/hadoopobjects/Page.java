package it.unipi.hadoop.hadoopobjects;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Page implements WritableComparable<Page> {
    private static double pageRank;
    private static String title;

    public Page(){
        pageRank = 0;
        title = "";
    }

    public void set(String title, double pageRank){
        setTitle(title);
        setPageRank(pageRank);
    }

    public void setTitle(String title){
        this.title = title;
    }

    public void setPageRank(double pageRank){
        this.pageRank = pageRank;
    }

    public String getTitle(){
        return title;
    }

    public double getPageRank(){
        return pageRank;
    }
    
    public void setByJson(final String json) {
        Page fromJson = new Gson().fromJson(json, Page.class);
        set(fromJson.getTitle(), fromJson.getRank());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.title);
        out.writeDouble(this.pageRank);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.pageRank = in.readDouble();
    }
    
    @Override
    public int hashCode() {
        return this.title.hashCode();
    }
    
    
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (!(o instanceof Page)) {
            return false;
        }

        Page that = (Page) o;
        return that.getTitle().equals(this.title)
                && this.rank == that.getRank();
    }
    
    @Override
    public String toString() {
        String json = new Gson().toJson(this);
        return json;
    }

    @Override
    public int compareTo(Page that) {
        double thatRank = that.getRank();
        String thatTitle = that.getTitle();
        return this.rank < thatRank ? 1 : (this.rank == thatRank ? this.title.compareTo(thatTitle) : -1);
    }

   /* public int compareTo(Page target) {
        double rank = target.getPageRank();
        String title = target.getTitle();
        return this.getPageRank() < rank ? 1 : (this.getPageRank() == rank ? this.getTitle().compareTo(title) : -1);
    }*/
}
