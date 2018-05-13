package ru.mipt.java2017.hw2;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mipt.java2017.sm06.PrimesSumGrpc;
import ru.mipt.java2017.sm06.PrimesSumProto;

/**
 * Class calculates the sum of primes on a segment.
 * @author Sofya Ozhereleva 697
 */

public class Server {

  private static final Logger logger = LoggerFactory.getLogger("Server");

  private io.grpc.Server server;
  private static int numberOfThreads;

  private void start(int port) throws IOException {
    /* The port on which the server should run */
    server = ServerBuilder.forPort(port)
        .addService(new NumberOfPrimesImpl())
        .build()
        .start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logger.error("*** shutting down gRPC server since JVM is shutting down");
        Server.this.stop();
        logger.error("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  /**
   * Initialize server.
   * @param args input
   * args[0] - number of available processor cores, args[1] - port
   * @throws IOException
   * @throws InterruptedException
   */

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length != 2) {
      logger.error("Incorrect server arguments");
      System.exit(1);
    }
    final Server server = new Server();
    Server.numberOfThreads = Integer.parseInt(args[0]);
    server.start(Integer.parseInt(args[1]));
    server.blockUntilShutdown();
  }

  /**
   * Class calculates the sum of primes on a segment.
   */

  static class primesSum implements Callable<Long> {

    private long begin;
    private long end;

    /**
     * Class constructor specifying the begin and the end of sub-segment to create.
     * @param begin the begin of the sub-segment
     * @param end the end of the sub-segment
     */
    private primesSum(long begin, long end) {
      this.begin = begin;
      this.end = end;
    }

    @Override
    public Long call() {
      long result = 0;
      for (long current = begin; current <= end; current++) {
        boolean isPrime = true;
        if(current == 1){
          isPrime = false;
        }
        for (long i = 2; i * i <= end; i++) {
          if (current % i == 0 && current != i) {
            isPrime = false;
            break;
          }
        }
        if (isPrime) {
          result += current;
        }
      }
      logger.info("from {} to {} return {}", begin, end, result);
      return result;
    }
  }

  /**
   * Split the segment to sub-segments, use ThreadPoll to handle asynchronous calculations.
   * @param begin the begin of the segment
   * @param end the end of the segment
   * @return the sum of primes
   */

  private static long findPrimesSum(long begin, long end) {
    if((long)numberOfThreads > end - begin + 1){
      numberOfThreads = (int)(end - begin + 1);
    }
    ExecutorService threadPool = Executors.newFixedThreadPool(numberOfThreads);
    List<primesSum> tasks = new ArrayList<>();
    long step = (end - begin + 1) / numberOfThreads;
    long modulo = (end - begin + 1) % numberOfThreads;
    long newBegin = begin;
    long newEnd = newBegin + step - 1;
    if (modulo > 0) {
      newEnd += 1;
      modulo--;
    }
    for (int i = 0; i < numberOfThreads; i++) {
      tasks.add(new primesSum(newBegin, newEnd));
      newBegin = newEnd + 1;
      newEnd = newBegin + step - 1;
      if (modulo > 0) {
        newEnd += 1;
        modulo--;
      }
    }
    long result = 0;
    List<Future<Long>> futures = new ArrayList<>();
    try {
      futures = threadPool.invokeAll(tasks);
    }catch (InterruptedException e) {
      logger.error("InterruptedException");
      System.exit(1);
    }
    try {
      for(Future<Long> f : futures){
        try{
          result += f.get();
        }catch (ExecutionException e){
          logger.error("ExecutionException");
          System.exit(1);
        }
      }
    }catch (InterruptedException e) {
      logger.error("InterruptedException");
      System.exit(1);
    }
    return result;
  }

  static class NumberOfPrimesImpl extends PrimesSumGrpc.PrimesSumImplBase {

    @Override
    public void calculateSum(PrimesSumProto.PrimesRequest request,
        StreamObserver<PrimesSumProto.PrimesReply> responseObserver) {
      long result = findPrimesSum(request.getBegin(), request.getEnd());
      PrimesSumProto.PrimesReply reply = PrimesSumProto.PrimesReply.newBuilder().setSum(result)
          .build();
      logger.info("Got request from client: from {} to {}", request.getBegin(), request.getEnd());
      responseObserver.onNext(reply);
      responseObserver.onCompleted();
    }
  }
}
