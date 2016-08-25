package com.alibaba.middleware.race.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public class FileUtil {

    public static void createDir(final String absDirName) {
        final File dir = new File(absDirName);
        if (!dir.exists() || !dir.isDirectory()) {
            dir.mkdir();
        }
    }

    public static String readFile(final String absFileName) {
        final File file = new File(absFileName);
        if (!file.exists()) {
            return null;
        }
        BufferedReader bufferedReader;
        final StringBuilder sb = new StringBuilder();
        try {
            bufferedReader = new BufferedReader(new FileReader(
                    file.getAbsoluteFile()));
            String string;
            try {
                while ((string = bufferedReader.readLine()) != null) {
                    sb.append(string + "\n");
                }
            } finally {
                bufferedReader.close();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void writeFile(final String absFileName, final String content) {
        BufferedWriter bufferedWriter;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(new File(
                    absFileName).getAbsoluteFile()));
            try {
                bufferedWriter.write(content);
            } finally {
                bufferedWriter.close();
            }
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public static void appendLineToFile(final String absFileName,
            final String content) {
        BufferedWriter bufferedWriter;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(new File(
                    absFileName).getAbsoluteFile(), true));
            try {
                bufferedWriter.write(content.concat("\n"));
            } finally {
                bufferedWriter.close();
            }
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isFileExist(final String absFileName) {
        final File file = new File(absFileName);
        if (file.exists()) {
            return true;
        }
        return false;
    }

    public static boolean deleteFileIfExist(final String absFileName) {
        final File file = new File(absFileName);
        if (file.exists()) {
            return file.delete();
        }
        return true;
    }

    /**
     * @param expectedResultFile
     * @return
     */
    public static TreeMap<String, Double> readHashMapFromFile(
            String pathname, int initialCapacity) {
        TreeMap<String, Double> retMap = new TreeMap<String, Double>();
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(pathname));
            String line = null;
            try {
                while ((line = bufferedReader.readLine()) != null) {
                    String[] splitOfLine = line.split(":");
                    if (splitOfLine.length == 2) {
                        retMap.put(splitOfLine[0].trim(), Double.parseDouble(splitOfLine[1].trim()));
                    } else {
                        throw new IOException("This line is not valid! : " + line);
                    }
                }
            } finally {
                bufferedReader.close();
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }
        return retMap;
    }
}
