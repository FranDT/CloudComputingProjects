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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(title);
        out.writeDouble(pageRank);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        title = in.readUTF();
        pageRank = in.readDouble();
    }
    
    @Override
    public int hashCode() {
        return this.title.hashCode();
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

    @Override
    public int compareTo(Page target) {
        double rank = target.getPageRank();
        String title = target.getTitle();
        return this.getPageRank() < rank ? 1 : (this.getPageRank() == rank ? this.getTitle().compareTo(title) : -1);
    }
}
