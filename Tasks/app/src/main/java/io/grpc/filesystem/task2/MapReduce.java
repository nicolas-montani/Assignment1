/*
 * the MapReduce functionality implemented in this program takes a single large text file to map i.e. split it into small chunks and then assign 1 to all the found words
 * then reduces by adding count values to each unique words
*/

package io.grpc.filesystem.task2;

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
     * @param inputfilepath
     * @throws IOException
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


        //open the file
        File file = new File(inputfilepath);
        Scanner sc = new Scanner(file);

        //create a Stringlist to store the words and count should always be 1
        List<String> map = new ArrayList<String>();

        //read the file line by line
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            //filter out punctuation
            line = line.replaceAll("\\p{Punct}|^[a-zA-Z0-9]", "");
            String[] words = line.split("\\s");
            for (String word : words) {
                // add the word and count to the map only if the words not equal to ""
                if (!word.equals(""))
                    map.add(word + ":1");
            }
        }


        //close the scanner
        sc.close();

        // safe to file input/temp/map/map-chunk001.txt if it already exist it will be written input/temp/map/map-chunk002.txt

        //create a file to store the map output
        int num = 1;

        // check if the file exist
        File mapFile = new File("input/temp/map/map-chunk" + String.format("%03d", num) + ".txt");
        while (mapFile.exists()) {
            mapFile = new File("input/temp/map/map-chunk" + String.format("%03d", num) + ".txt");
            num++;
        }

        //create a file writer
        FileWriter writer = new FileWriter(mapFile);

        //write the map output to the file
        for (String string : map) {
            writer.write(string + "\n");
        }

        //close the file writer
        writer.close();


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