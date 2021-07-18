package it.unipi.hadoop.hadoopobjects;

import com.google.gson.Gson;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 *
 * Node implements the Writable interface, that implements serialization and deserialization of data between map and reduce
 * phases.
 *
 */
public class Node implements Writable {
    private double pageRank;
    private List<String> outlinks;
    private boolean isNode;

    public Node(){
        setPageRank(0);
        setAdjacencyList(new LinkedList<String>());
        setIsNode(false);
    }

    public void setPageRank(final double pageRank){
        this.pageRank = pageRank;
    }

    public void setAdjacencyList(final List<String> outlinks){
        this.outlinks = outlinks;
    }

    public void setIsNode(final boolean value) { this.isNode = value; }

    public void set(final double pageRank, final List<String> outlinks, final boolean isNode){
        setPageRank(pageRank);
        setAdjacencyList(outlinks);
        setIsNode(isNode);
    }

    public void set(final Node node){
        setPageRank(node.getPageRank());
        setAdjacencyList(node.getAdjacencyList());
        setIsNode(node.getIsNode());
    }

    public double getPageRank(){
        return pageRank;
    }

    public List<String> getAdjacencyList(){
        return outlinks;
    }

    public boolean getIsNode() { return isNode; }

    //Method to be implemented from Writable interface

    public void write(DataOutput out) throws IOException{
        out.writeDouble(pageRank);

        //I write also the size since I need it to retrieve all the outlinks during the readFields
        out.writeInt(outlinks.size());
        for(String outNode : outlinks){
            out.writeUTF(outNode);
        }
        out.writeBoolean(isNode);
    }

    public void readFields(DataInput in) throws IOException {
        pageRank = in.readDouble();

        int listSize = in.readInt();
        outlinks = new LinkedList<String>();
        while(listSize > 0){
            outlinks.add(in.readUTF());
            listSize--;
        }

        isNode = in.readBoolean();
    }

    /**
     *
     * The toString() method is customized for this class for ease of data retrieval from file: in fact, in the rank mapper
     * we need to read the nodes from a file, having the Text format: to obtain and build the Node object, we can convert
     * the file into a Json string and parse it easily with the Json utilities.
     *
     * @return
     */
    @Override
    public String toString(){
        return new Gson().toJson(this);
    }

    public void setByJson(final String json){
        Double pageRank = Double.parseDouble(json.substring(json.indexOf("pagerank\":") + 10, json.indexOf("outlinks\":") - 2));

        String outlinks = json.substring(json.indexOf("outlinks\":") + 11, json.indexOf("isNode\":") - 3);
        List<String> outlinksList = new ArrayList<String>();
        String[] toParse = outlinks.split(",");
        for(String s : toParse){
            outlinksList.add(s);
        }

        boolean isNode = Boolean.getBoolean(json.substring(json.indexOf("isNode\":") + 8));

        this.set(pageRank, outlinksList, isNode);
    }
}
