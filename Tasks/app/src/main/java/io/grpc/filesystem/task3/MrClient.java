/* 
* Client program to request for map and reduce functions from the Server
*/

package io.grpc.filesystem.task3;
import java.io.File;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

import java.io.*;
import java.nio.charset.Charset;

public class MrClient {
   Map<String, Integer> jobStatus = new HashMap<String, Integer>();

   public  void requestMap(String ip, Integer portnumber, String inputfilepath, String outputfilepath) throws InterruptedException {
      
      /* 
      * Insert your code here 
      * Create a stub for calling map function from the server
      * Remember that the map function uses client stream
      * Update the job status every time the map function finishes mapping a chunk, it is useful for calling reduce function once all of the chunks are processed by the map function
      */
       System.out.println(inputfilepath);

      // create channel, asyncstud
       ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build();
       AssignJobGrpc.AssignJobStub stub = AssignJobGrpc.newStub(channel);


       // create a response observer to handle responses from the server
       StreamObserver<MapInput> requestObserver = stub.map(new StreamObserver<MapOutput>() {
           @Override
           public void onNext(MapOutput mapOutput) {

               // Handle the response from the server
               int jobStatusValue = mapOutput.getJobstatus();
               // Update the job status for this chunk
               jobStatus.put(inputfilepath, jobStatusValue);

           }
            @Override
            public void onError(Throwable t) {
                System.err.println("Clientside Error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                channel.shutdown();
                System.out.println("Map Completed");
            }

        });



       MapInput mapInput = MapInput.newBuilder()
                   .setIp(ip)
                   .setPort(portnumber)
                   .setInputfilepath(inputfilepath)
                   .setOutputfilepath(outputfilepath)
                   .build();

       requestObserver.onNext(mapInput);
       requestObserver.onCompleted();

       channel.awaitTermination(5, TimeUnit.SECONDS);



       // Create a MapInput message and send it to the server

   }




   public int requestReduce(String ip, Integer portnumber, String inputfilepath, String outputfilepath) {
       
      /* 
      * Insert your code here 
      * Create a stub for calling reduce function from the server
      * Remember that the map function uses unary call
      */
       System.out.println("reduce function is executed");
       System.out.println(inputfilepath);

       // Create a gRPC channel to connect to the server
       ManagedChannel channel = ManagedChannelBuilder.forAddress(ip, portnumber).usePlaintext().build();

       // Create a stub for the AssignJob service
       AssignJobGrpc.AssignJobBlockingStub blockingStub = AssignJobGrpc.newBlockingStub(channel);

       // Create a ReduceInput message to send to the server
       ReduceInput reduceInput = ReduceInput.newBuilder()
               .setIp(ip)
               .setPort(portnumber)
               .setInputfilepath(inputfilepath)
               .setOutputfilepath(outputfilepath)
               .build();

       try {
           // Call the reduce function on the server and receive a ReduceOutput response
           ReduceOutput reduceOutput = blockingStub.reduce(reduceInput);

           // Return the job status
           int jobStatus = reduceOutput.getJobstatus();

           return jobStatus;
       } catch(Exception e) {
           // Handle errors, such as gRPC exceptions
           System.err.println("Error in reduce function: " + e.getMessage());
           return -1; // Return a negative value to indicate an error
       }
   }


   public static void main(String[] args) throws Exception {// update main function if required

      String ip = args[0];
      Integer mapport = Integer.parseInt(args[1]);
      Integer reduceport = Integer.parseInt(args[2]);
      String inputfilepath = args[3];
      String outputfilepath = args[4];
      String jobtype = null;
      MrClient client = new MrClient();
      int response = 0;

      MapReduce mr = new MapReduce();
      String chunkpath = mr.makeChunks(inputfilepath);
      Integer noofjobs = 0;
      File dir = new File(chunkpath);
      File[] directoyListing = dir.listFiles();
      if (directoyListing != null) {
         for (File f : directoyListing) {
            if (f.isFile()) {
               noofjobs += 1;
               client.jobStatus.put(f.getPath(), 1);
                client.requestMap(ip, mapport, f.getPath(), outputfilepath);
            }
         }
      }


      Set<Integer> values = new HashSet<Integer>(client.jobStatus.values());

      if (values.size() == 1 && client.jobStatus.containsValue(2)) {
         response = client.requestReduce(ip, reduceport, chunkpath, outputfilepath);
         if (response == 2) {

            System.out.println("Reduce task completed!");

         } else {
            System.out.println("Try again! " + response);
         }

      }

   }

}
