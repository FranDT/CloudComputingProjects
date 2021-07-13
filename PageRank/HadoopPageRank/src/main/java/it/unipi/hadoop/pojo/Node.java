package it.unipi.hadoop.pojo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class Node implements Writable {
    private double pageRank;
    private List<String> outlinks;

    public Node(){
        pageRank = 0;
        setAdjacencyList(new LinkedList<String>());
    }

    public Node(final double pageRank, final List<String> outlinks){
        set(pageRank, outlinks);
    }

    public void setPageRank(final double pageRank){
        this.pageRank = pageRank;
    }

    public void setAdjacencyList(final List<String> outlinks){
        this.outlinks = outlinks;
    }

    public void set(final double pageRank, final List<String> outlinks){
        setPageRank(pageRank);
        setAdjacencyList(outlinks);
    }

    public void set(final Node node){
        setPageRank(node.getPageRank());
        setAdjacencyList(node.getAdjacencyList());
    }

    public double getPageRank(){
        return pageRank;
    }

    public List<String> getAdjacencyList(){
        return outlinks;
    }

    //Method to be implemented from Writable interface

    public void write(DataOutput out) throws IOException{
        out.writeDouble(pageRank);

        //I write also the size since I need it to retrieve all the outlinks during the readFields
        out.writeInt(outlinks.size());
        for(String outNode : outlinks){
            out.writeUTF(outNode);
        }
    }

    public void readFields(DataInput in) throws IOException {
        pageRank = in.readDouble();

        int listSize = in.readInt();
        outlinks = new LinkedList<String>();
        while(listSize > 0){
            outlinks.add(in.readUTF());
            listSize--;
        }
    }
}
