package it.unipi.hadoop.parser;

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

    public List<String> getOulinks(final String inputString) {
        List<String> outlinks = new LinkedList<String>();

        Pattern pattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
        this.matcher = pattern.matcher(inputString);

        while(matcher.find())
            outlinks.add(matcher.group(1).replace("\t", ""));

        return outlinks;
    }
}
