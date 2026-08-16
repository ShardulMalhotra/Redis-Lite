package com.shardul.redislite.protocol;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns a raw line like:  SET user:1 "shardul malhotra" EX 60
 * into tokens:            ["SET", "user:1", "shardul malhotra", "EX", "60"]
 *
 * Real Redis uses a binary-safe length-prefixed protocol (RESP) so values
 * can contain literally any bytes, including spaces and newlines. We use
 * a simpler space-delimited text protocol with basic quote support —
 * easier to debug with telnet/netcat, at the cost of not being fully
 * binary-safe. That trade-off is worth stating explicitly if asked.
 */
public class CommandParser {

    public static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ' ' && !inQuotes) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }
}
