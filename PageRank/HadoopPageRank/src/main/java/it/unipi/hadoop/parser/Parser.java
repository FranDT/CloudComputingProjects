package it.unipi.hadoop.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
    Matcher matcher;

    public Parser(){}

    public String getTitle(String inputString){
        Pattern pattern = Pattern.compile("<title.*?>(.*?)</title>");
        this.matcher = pattern.matcher(inputString);

        if(matcher.find()){
            return matcher.group(1).replace("\t", "");
        }
        return null;
    }

    public List<String> getOulinks(final String fileToSearch) {
        List<String> outlinks = new LinkedList<String>();

        try {
            //String line;
            String[] arr;
            //BufferedReader bufferedReader = new BufferedReader(new FileReader(fileToSearch));
            arr = fileToSearch.split("\\[\\[");
            for (int i = 0; i < arr.length; i++) {
                if (arr[i].contains("]]")) {
                    outlinks.add(arr[i].substring(0, arr[i].indexOf("]]")));
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        return new LinkedList<String>(outlinks);
    }
}
