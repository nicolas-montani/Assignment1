
/* 
* gRPC server node to accept calls from the clients and serve based on the method that has been requested
*/

package io.grpc.filesystem.task3;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.task3.proto.AssignJobGrpc;
import com.task3.proto.MapInput;
import com.task3.proto.ReduceInput;
import com.task3.proto.MapOutput;
import com.task3.proto.ReduceOutput;
import io.grpc.filesystem.task2.*;

public class MrReduceServer {

    private Server server;

    private void start(int port) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MrReduceServerImpl()).build().start();
        System.out.println("Listening on: " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("Terminating the server at port: " + port);
                try {
                    server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }
        });
    }

    static class MrReduceServerImpl extends AssignJobGrpc.AssignJobImplBase {

        /*
         * Insert your code here
         * Override the reduce function to accept the request and perform Reduce
         * Take help of autogenerated file Assignment1/BCS-DS-Assignment-Package/Task3/app/build/generated/source/proto/main/grpc/com/task3/proto/AssignJobGrpc.java
         * Look through the available functions and definition in this Grpc file to complete the task
         */

        MapReduce mapReduce = new MapReduce();


        @Override
        public void reduce(ReduceInput request, StreamObserver<ReduceOutput> responseObserver) {
            if (request == null || request.getInputfilepath().isEmpty()) {
                responseObserver.onError(new IllegalArgumentException("Request or FileName is invalid")); // handle invalid file
                return;
            }

            // extract the input and output file names from the request
            String input = request.getInputfilepath();
            String output = request.getOutputfilepath();

            try {
                //perform reduce task
                mapReduce.reduce(input, output);

                // Create a ReduceOutput response with the job status
                ReduceOutput response = ReduceOutput
                        .newBuilder()
                        .setJobstatus(2)
                        .build();

                // Send the response back to the client
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (Exception e) {
                // Handle any exceptions that may occur during the reduce operation
                System.err.println("Error during reduce operation: " + e.getMessage());

                // Send an error response to the client
                ReduceOutput response = ReduceOutput.newBuilder()
                        .setJobstatus(-1)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        }
    }


        public static void main(String[] args) throws IOException, InterruptedException {
            final MrReduceServer mrServer = new MrReduceServer();
            for (String i : args) {

                mrServer.start(Integer.parseInt(i));

            }
            mrServer.server.awaitTermination();
        }
    }