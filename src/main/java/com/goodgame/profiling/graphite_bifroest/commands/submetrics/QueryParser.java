package com.goodgame.profiling.graphite_bifroest.commands.submetrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryParser {
    private static final Logger log = LogManager.getLogger();

    private String query;
    private int curPos = 0;
    
    // List of all the metric names found in the query.
    // Still contains things like *, {} and [].
    private List<String> metricNamesFound = new ArrayList<>();
    
    public QueryParser(String query) {
        this.query = query;
    }
    
    public List<String> parse() {
        parseExpression();
        
        for (int i = 0; i < metricNamesFound.size(); i++) {
            metricNamesFound.set(i, metricNamesFound.get(i).trim());
        }
        
        return metricNamesFound;
    }
    
    public void parseExpression() {
        int startOfExpression = curPos;
        
        while(curPos < query.length()) {
            log.trace("parseExpression " + curPos + " " + query.charAt(curPos));
            
            switch(query.charAt(curPos)) {
            case '(':
                curPos++;
                parseFunctionCall();
                return;
            case ')':
            case ',':
                metricNamesFound.add(query.substring(startOfExpression, curPos));
                return;
            case '"':
                int positionOfClosingQuotes = query.indexOf('"', curPos+1);
                if (positionOfClosingQuotes >= 0) {
                    curPos = positionOfClosingQuotes +1;
                    break;
                } else {
                    throw new IllegalStateException("No closing \" found");
                }
            case '{':
                int positionOfClosingBrace = query.indexOf('}', curPos+1);
                if (positionOfClosingBrace >= 0) {
                    curPos = positionOfClosingBrace +1;
                    break;
                } else {
                    throw new IllegalStateException("No closing } found");
                }
            default:
                curPos++;
            }
        }
        
        metricNamesFound.add(query.substring(startOfExpression, curPos));
    }
    
    public void parseFunctionCall() {
        log.info("Interesting query! " + query);
        
        while(true) {
            if (curPos >= query.length()) {
                throw new IllegalStateException("parseFunctionCall past end of query string.");
            }
            
            log.trace("parseFunctionCall " + curPos + " " + query.charAt(curPos));
            
            switch(query.charAt(curPos)) {
            case ')':
                curPos++;
                return;
            case ',':
                curPos++;
                //$FALL-THROUGH$
            default:
                parseExpression();
            }
        }
    }
}
