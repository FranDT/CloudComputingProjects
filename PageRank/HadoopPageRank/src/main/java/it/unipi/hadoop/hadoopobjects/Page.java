package it.unipi.hadoop.hadoopobjects;

<<<<<<< HEAD
import com.google.gson.Gson;
=======
>>>>>>> b222e6c3fabacfef1e08ac2a93153a1c1201ae43
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Page implements WritableComparable<Page> {
<<<<<<< HEAD
    private String title;
    private double rank;

    //-------------------------------------------------------------------------------

    public Page() { }

    public Page(final String title, final double rank) {
        set(title, rank);
    }

    //-------------------------------------------------------------------------------

    public void setTitle(final String title) { this.title = title; }

    public void setRank(final double rank) { this.rank = rank; }

    public void set(final String title, final double rank) {
        setTitle(title);
        setRank(rank);
    }

    public void setFromJson(final String json) {
        Page fromJson = new Gson().fromJson(json, Page.class);
        set(fromJson.getTitle(), fromJson.getRank());
    }

    public String getTitle() { return this.title; }

    public double getRank() { return this.rank; }

    //-------------------------------------------------------------------------------

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.title);
        out.writeDouble(this.rank);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.title = in.readUTF();
        this.rank = in.readDouble();
    }

    //-------------------------------------------------------------------------------

    public String toHumanString() { return "[Title: " + title + "]\t" + rank; }

    @Override
    public String toString() {
        String json = new Gson().toJson(this);
        return json;
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
    public int hashCode() { return this.title.hashCode(); }

    @Override
    public int compareTo(Page that) {
        double thatRank = that.getRank();
        String thatTitle = that.getTitle();
        return this.rank < thatRank ? 1 : (this.rank == thatRank ? this.title.compareTo(thatTitle) : -1);
    }
=======
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

    public void write(DataOutput out) throws IOException {
        out.writeUTF(title);
        out.writeDouble(pageRank);
    }

    public void readFields(DataInput in) throws IOException {
        title = in.readUTF();
        pageRank = in.readDouble();
    }
    
    @Override
    public int hashCode() {
        return this.title.hashCode() + new Double(this.pageRank).hashCode();
    }
    
    
    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Page){
            Page o = (Page)obj;
            return this.title.equals(o.getTitle()) && this.pageRank == o.getPageRank();
        }
        return false;
    }
    
    @Override
    public String toString() {
        return "Title:" + this.title + "\tRank:" + this.pageRank;
    }

    public int compareTo(Page o) {
        double mis = (this.pageRank - o.getPageRank());
        if(mis > 0 ){
            return 1;
        } else if (mis < 0){
            return -1;   
        }
        else{
            return this.getTitle().compareTo(o.getTitle());
        }
    }

   /* public int compareTo(Page target) {
        double rank = target.getPageRank();
        String title = target.getTitle();
        return this.getPageRank() < rank ? 1 : (this.getPageRank() == rank ? this.getTitle().compareTo(title) : -1);
    }*/
>>>>>>> b222e6c3fabacfef1e08ac2a93153a1c1201ae43
}
