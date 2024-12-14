package com.coroptis.index.benchmark;

import java.util.Random;

public class DataProvider {

    private final static Random RANDOM = new Random();
    private final static int RANDOM_STRING_LENGTH = 20;
    private final String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    String generateRandomString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < RANDOM_STRING_LENGTH; i++) {
            int randomIndex = RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(randomIndex));
        }
        return sb.toString();
    }

}
