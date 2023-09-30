/*
 * the MapReduce functionality implemented in this program takes a single large text file to map i.e. split it into small chunks and then assign 1 to all the found words
 * then reduces by adding count values to each unique words
*/

package io.grpc.filesystem.task2;

import java.sql.SQLOutput;
import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Map.Entry;

import io.grpc.filesystem.task2.Mapper;

public class MapReduce {

    public MapReduce() throws IOException {
    }

    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        int size = 500;
        File f = new File(inputFilePath);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String l = br.readLine();

            while (l != null) {
                File newFile = new File(f.getParent() + "/temp", "chunk"
                        + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (l != null) {
                        byte[] bytes = (l + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > size)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        l = br.readLine();
                    }
                }
            }
        }
        return f.getParent() + "/temp";

    }

    /**
     * @param inputfilepath Takes a text file as an input and returns counts of each
     * @throws IOException throws Exception
     */

    public static void map(String inputfilepath) throws IOException {

        /*
         * Insert your code here
         * Take a chunk and filter words (you could use "\\p{Punct}" for filtering punctuations and "^[a-zA-Z0-9]"
         * together for filtering the words), then split the sentences to take out words and assign "1" as the initial count.
         * Use the given mapper class to create the unsorted key-value pair.
         * Save the map output in a file named "map-chunk001", for example, in folder
         * path input/temp/map
         */


        //select inputfile
        File inputFile = new File(inputfilepath);

        //select tempFile
        String tempFolder = inputFile.getParentFile().getParent() + "/temp/map";
        String tempFileName = "map-" + inputFile.getName();
        File tempFile = new File(tempFolder, tempFileName);

        //bufferReader for both files

        BufferedReader brInput = new BufferedReader(new FileReader(inputFile));
        BufferedWriter bwTemp = new BufferedWriter(new FileWriter(tempFile));

        //create a Stringlist to store the words and count should always be 1
        List<String> map = new ArrayList<String>();

            //print each line
        String line;
        while ((line = brInput.readLine()) != null) {
            //split line in words
            String words[] = line.split("\\s+");

            for (String word : words) {
                word = word.replaceAll("\\p{Punct}", "").toLowerCase();

                //add to list
                if (word.matches("^[a-zA-Z0-9]*$") && !word.equals("")) {
                    map.add(word + ":1");
                }


            }
        }

        // add each word to bufferWriter

        for (String word : map) {
            bwTemp.write(word + "\n");
        }

        //close the bufferReader and bufferWriter
        brInput.close();
        bwTemp.close();


    }


    /**
     * @param inputfilepath
     * @param outputfilepath
     * @return
     * @throws IOException
     */
    public static void reduce(String inputfilepath, String outputfilepath) throws IOException {

        /*
         * Insert your code here
         * Take all the files in the map folder and reduce them to one file that shows
         * unique words with their counts as "the:64", for example.
         * Save the output of reduce function as output-task2.txt
         */

        //select each inputfile that end with txt in inputfilepath + "/map"
        File inputFolder = new File(inputfilepath + "/map");
        File[] inputFiles = inputFolder.listFiles((dir, name) -> name.endsWith(".txt"));

        //select outputfile
        File outputFile = new File(outputfilepath);


        //create a map to store the words and count
        HashMap<String, Integer> reduce = new HashMap<>();

        //read each inputfile
        for (File inputFile : inputFiles) {
            BufferedReader brInput = new BufferedReader(new FileReader(inputFile));

            //read each line
            String line;
            while ((line = brInput.readLine()) != null) {
                //split line in words
                String words[] = line.split(":");

                //add to map
                if (reduce.containsKey(words[0])) {
                    reduce.put(words[0], reduce.get(words[0]) + Integer.parseInt(words[1]));
                } else {
                    reduce.put(words[0], Integer.parseInt(words[1]));
                }
            }
            brInput.close();
        }

        // sort hashmap by integer
        reduce = reduce.entrySet().stream().sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

        //write to outbutfile

        BufferedWriter bwOutput = new BufferedWriter(new FileWriter(outputFile));

        for (Map.Entry<String, Integer> entry : reduce.entrySet()) {
            bwOutput.write(entry.getKey() + ":" + entry.getValue() + "\n");
        }

        bwOutput.close();

    }

    /**
     * Takes a text file as an input and returns counts of each word in a text file
     * "output-task2.txt"
     * 
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException { // update the main function if required
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        String chunkpath = makeChunks(inputFilePath);
        File dir = new File(chunkpath);
        File[] directoyListing = dir.listFiles();
        if (directoyListing != null) {
            for (File f : directoyListing) {
                if (f.isFile()) {

                    map(f.getPath());

                }

            }

            reduce(chunkpath, outputFilePath);

        }

    }
}