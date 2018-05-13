package ru.mipt.java2017.hw2;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mipt.java2017.sm06.*;

/**
 * Class connected to server to calculate the sum of primes on a segment.
 * @author Sofya Ozhereleva 697
 */

public class Client {

  private static final Logger logger = LoggerFactory.getLogger("Client");

  private final ManagedChannel channel;
  private final PrimesSumGrpc.PrimesSumBlockingStub blockingStub;
  private static Integer numberOfThreads;

  private static Integer argLength;
  private static BlockingQueue<Segment> requests;
  private static long[] sum;

  /**
   * Class that contains and may return the begin
   * and the end of a segment.
   *
   */
  public static class Segment {

    private long begin;
    private long end;
    /**
     * Class constructor specifying the begin and the end of objects to create.
     */
    public Segment(long begin, long end) {
      this.begin = begin;
      this.end = end;
    }

    public long returnBegin() {
      return this.begin;
    }

    public long returnEnd() {
      return this.end;
    }
  }

  private Client(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext(true)
        .build());
  }

  private Client(ManagedChannel channel) {
    this.channel = channel;
    this.blockingStub = PrimesSumGrpc.newBlockingStub(channel);
  }

  private void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /**
   * Modifies the request queue, splitting a passing segment into sub-segments
   * and add them as the requests.
   * @param requests queue of requests
   * @param begin the begin of the segment to split
   * @param end the end of the segment to split
   */

  private static void CreateRequest(BlockingQueue<Segment> requests, long begin, long end) {
    Long step = (end - begin + 1) / numberOfThreads;
    Long modulo = (end - begin + 1) % numberOfThreads;
    Long newBegin = begin;
    Long newEnd = newBegin + step - 1;
    if (modulo > 0) {
      newEnd += 1;
      modulo--;
    }
    for (int i = 2; i <= numberOfThreads*2; i += 2) {
      try {
        requests.put(new Segment(newBegin, newEnd));
      } catch (InterruptedException e) {
        logger.error("Segmentation error");
        System.exit(1);
      }
      newBegin = newEnd + 1;
      newEnd = newBegin + step - 1;
      if (modulo > 0) {
        newEnd += 1;
        modulo--;
      }
    }
  }

  /**
   * <p>Try to get the result of the calculation from the server.
   * Writes the result to an public static array "sum", modifying it.</p>
   * <p>If server isDead call CreateRequest function to split it's
   * segment into more requests (the number of requests is equal to the number of living servers).</p>
   * @param begin the begin of the segment to count
   * @param end the end of the segment to count
   * @param counter the number of the thread to write the result of calculation to "sum" array
   * @param isDead the flag
   */

  private void countPrimes(Long begin, Long end, Integer counter, boolean isDead) {
    logger.info("Will try to count primes from {} to {}... ", begin, end);
    PrimesSumProto.PrimesRequest request = PrimesSumProto.PrimesRequest.newBuilder().setBegin(begin)
        .setEnd(end).build();
    PrimesSumProto.PrimesReply response;
    try {
      response = blockingStub.calculateSum(request);
    } catch (StatusRuntimeException e) {
      if(!isDead){
        argLength -= 2;
      }
      if (argLength == 0) {
        logger.error("All servers died");
        System.exit(1);
      }
      CreateRequest(requests, begin, end);
      logger.error("RPC failed: {}", e.getStatus());
      throw e;
    }
    sum[counter] += response.getSum();
  }

  static class MyThread extends Thread {

    private Integer port;
    private String host;
    private Integer counter;

    private MyThread(String host, Integer port, Integer counter) {
      this.host = host;
      this.port = port;
      this.counter = counter;
    }

    /**
     * <p>In the thread, the next segment is processed.</p>
     * <p>If the server disconnected, it tries to connect the server again
     * till the queue of requests is not empty.</p>
     */

    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      logger.info("Thread {} started", port);
      Client client = new Client(host, port);
      long begin = 0;
      long end = 0;
      boolean isDead = false;
      try {
        while (!requests.isEmpty() && !isDead) {
          try {
            try {
              Segment segment = requests.take();
              begin = segment.returnBegin();
              end = segment.returnEnd();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              argLength -= 2;
            }
            client.countPrimes(begin, end, counter, isDead);
          } catch (StatusRuntimeException e) {
            isDead = true;
            while(!requests.isEmpty()&& isDead){
              try{
                client.countPrimes((long)1, (long)2, counter, isDead);
                isDead = false;
                argLength += 2;
              } catch (StatusRuntimeException e1){
                logger.error("StatusRuntimeException");
                System.exit(1);
              }
            }
          }
        }
      } finally {
        try {
          client.shutdown();
          Thread.currentThread().interrupt();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      long timeSpent = System.currentTimeMillis() - startTime;
      logger.info("программа выполнялась {} миллисекунд", timeSpent);
    }
  }

  /**
   * Create the threads, one for each server.
   * @param args the segment and the list of ports and hosts
   * @throws Exception
   */

  public static void main(String[] args) throws Exception {
    List<Thread> threads = new LinkedList<>();
    if (!(args.length > 3 && args.length % 2 == 0)) {
      System.err.println("Incorrect client arguments");
      System.exit(1);
    }
    long begin = Long.parseLong(args[0]);
    long end = Long.parseLong(args[1]);
    Client.argLength = args.length - 2;
    numberOfThreads = argLength / 2;
    if(numberOfThreads > end - begin + 1){
      numberOfThreads = (int)(end - begin + 1);
    }
    sum = new long[numberOfThreads];
    requests = new ArrayBlockingQueue(argLength * argLength);
    CreateRequest(requests, begin, end);
    for (int i = 2; i <= numberOfThreads*2; i += 2) {
      threads.add(new MyThread(args[i], Integer.valueOf(args[i + 1]), i / 2 - 1));
    }
    threads.forEach(Thread::start);
    threads.forEach(thread -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    long result = 0;
    for (long res : sum) {
      result += res;
    }
    System.out.println(result);
  }
}
