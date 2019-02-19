package io.openmessaging;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultQueueStoreImpl extends QueueStore {

    private final int queueTotalNumber = 1000000;//100w个队列数
    private final int messageNumberPerQueue = 2000;//每个队列的消息数
    private final int fileNumber = 200;//文件个数
    private final int indexNumber = 1000000;


    private final int messageLength = 58;//每个消息的长度
    private final int messageNumberPerBuffer = 10;//缓冲区中每个消息的个数
    private final int directBufferNumber = 5;//缓冲区的个数
    private final int directBufferSize = 58 * 10 * queueTotalNumber;//直接缓冲区的大小，580*10^6=580M

    private ByteBuffer[] directBuffers = new ByteBuffer[directBufferNumber];//5个缓冲区
    private ByteBuffer[][] sliceBuffers = new ByteBuffer[directBufferNumber][];
    private volatile int[] sliceCurrentPositions = new int[queueTotalNumber];//当前队列写到哪一个缓冲块
    private volatile int currentFile = 0;
    private AtomicInteger[] currentCompleteSlice = new AtomicInteger[directBufferNumber];

    private FileChannel[] fileChannels = new FileChannel[fileNumber];
    private ArrayBlockingQueue<FileChannel>[] fileChannelPools;

    private final ArrayBlockingQueue<ByteBuffer> bufferPool;
    private final int buffersPoolSize = messageLength * messageNumberPerBuffer;
    private final int bufferPoolNumber = 10;//测评程序是分了两个线程池，每个线程池10个线程，进行读取

    private CountDownLatch[] countDownLatch = new CountDownLatch[fileNumber];

    private Map<String, Integer> queueMap = new ConcurrentHashMap<>();

    private Map<String, byte[][]> threadByteMap = new ConcurrentHashMap<>();
    private Map<String, List<byte[]>> threadListMap = new ConcurrentHashMap<>();

    public DefaultQueueStoreImpl() {

        for(int i = 0; i < queueTotalNumber; i++) {
            queueMap.put("Queue-" + i, i);
        }

        for(int i = 0; i < bufferPoolNumber; i++) {
            byte[][] bytes = new byte[bufferPoolNumber][];
            for(int j = 0; j < bufferPoolNumber; j++) {
                bytes[j] = new byte[messageLength];
            }
            threadByteMap.put("Thread-2" + i, bytes);
            threadByteMap.put("Thread-1" + i, bytes);
            threadListMap.put("Thread-2" + i, new ArrayList<>());
            threadListMap.put("Thread-1" + i, new ArrayList<>());
        }
        for(int bufferIndex = 0; bufferIndex < directBuffers.length; bufferIndex++) {
            directBuffers[bufferIndex] = ByteBuffer.allocateDirect(directBufferSize);
            sliceBuffers[bufferIndex] = new ByteBuffer[queueTotalNumber];
            currentCompleteSlice[bufferIndex] = new AtomicInteger(0);
            for (int sliceIdx = 0; sliceIdx < sliceBuffers[bufferIndex].length; sliceIdx++) {
                sliceBuffers[bufferIndex][sliceIdx] = directBuffers[bufferIndex].duplicate();
                sliceBuffers[bufferIndex][sliceIdx].position(messageLength * messageNumberPerBuffer * sliceIdx);
                sliceBuffers[bufferIndex][sliceIdx].limit(messageLength * messageNumberPerBuffer * (sliceIdx + 1));
                sliceBuffers[bufferIndex][sliceIdx].mark();
            }
        }

        for(int queueIdx = 0; queueIdx < queueTotalNumber; queueIdx++) {
            sliceCurrentPositions[queueIdx] = 0;
        }

        fileChannelPools = new ArrayBlockingQueue[fileNumber];
        try {
            for(int fileIdx = 0; fileIdx < fileChannels.length; fileIdx++) {
                fileChannels[fileIdx] = new RandomAccessFile(String.valueOf(fileIdx), "rw").getChannel();
                fileChannelPools[fileIdx] = new ArrayBlockingQueue<FileChannel>(bufferPoolNumber);
                for(int i = 0; i < bufferPoolNumber; i++) {
                    fileChannelPools[fileIdx].offer(new RandomAccessFile(String.valueOf(fileIdx), "r").getChannel());
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        bufferPool = new ArrayBlockingQueue<>(bufferPoolNumber,true);
        for (int i = 0; i < bufferPoolNumber; i++) {
            bufferPool.offer(ByteBuffer.allocateDirect(buffersPoolSize));
        }

        for(int i = 0; i < countDownLatch.length; i++) {
            countDownLatch[i] = new CountDownLatch(1);
        }
    }

    private int writeBuffer(int queueNumber, byte[] message) {
        int currentPosition = sliceCurrentPositions[queueNumber];
        if(!sliceBuffers[currentPosition][queueNumber].hasRemaining()) {
            currentPosition = (currentPosition + 1) % directBufferNumber;
            sliceCurrentPositions[queueNumber] = currentPosition;
        }

        sliceBuffers[currentPosition][queueNumber].put(message);
        if(!sliceBuffers[currentPosition][queueNumber].hasRemaining()) {
            sliceCurrentPositions[queueNumber] = (currentPosition + 1) % directBufferNumber;
            return currentCompleteSlice[currentPosition].incrementAndGet();
        }
        return -1;
    }

    public void put(String queueName, byte[] message) {
        int queueNumber = queueMap.get(queueName);

        if (writeBuffer(queueNumber, message) == queueTotalNumber) {
            int directBuffer = currentFile % directBufferNumber;
            currentCompleteSlice[directBuffer].set(0);
            directBuffers[directBuffer].clear();
            try {
                fileChannels[currentFile].write(directBuffers[directBuffer]);
                fileChannels[currentFile].close();
                fileChannels[currentFile] = new RandomAccessFile(String.valueOf(currentFile), "r").getChannel();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(fileNumber - currentFile <= directBufferNumber) {
                //写到最后5个文件的时候
                directBuffers[directBuffer].clear();
                try {
                    fileChannels[directBuffer].read(directBuffers[directBuffer]);
                    countDownLatch[directBuffer].countDown();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else {
                for(int sliceIdx = 0; sliceIdx < sliceBuffers[directBuffer].length; sliceIdx++) {
                    sliceBuffers[directBuffer][sliceIdx].reset();
                }
            }
            currentFile++;
        }
    }

    private volatile int from = 0;
    private volatile int interval = 4;
    private AtomicInteger getNumber = new AtomicInteger(0);
    private volatile boolean isIndexOver = false;

    private AtomicInteger check = new AtomicInteger(0);

    public Collection<byte[]> get(String queueName, long offset, long num) {

        String threadName = Thread.currentThread().getName();
        byte[][] bytes = threadByteMap.get(threadName);//用来读取缓存中的消息
        int bytePos = 0;

        if(!isIndexOver && getNumber.incrementAndGet() == indexNumber + 1) {
            isIndexOver = true;//第二阶段结束的标志
        }

        List<byte[]> ans = threadListMap.get(threadName);
        ans.clear();
        int queueNumber = queueMap.get(queueName);

        int msgOffset = (int)offset;
        int msgNumber = (int)num;

        while(msgNumber > 0 && msgOffset < messageNumberPerQueue) {

            int queryStartInBuffer = msgOffset % messageNumberPerBuffer; //获取消息offset对10的余数，第几个消息
            int fileIdx = msgOffset / messageNumberPerBuffer;//要读的哪个文件
            int canReadNum = (messageNumberPerBuffer - queryStartInBuffer > msgNumber)? msgNumber: (messageNumberPerBuffer - queryStartInBuffer); //第一步读取消息个数
            int startPosition = messageLength * (queueNumber * messageNumberPerBuffer + queryStartInBuffer);//文件的开始的读取的位置

            if(isIndexOver && fileIdx == from + interval && check.getAndIncrement() == 0) {
                //from依次增加1，五个缓存块依次替换
                int bufferNum = from % directBufferNumber;
                int fileNum = from + directBufferNumber;
                if(fileNum < fileNumber)  {
                    from++;
                    check.set(0);
                    directBuffers[bufferNum].clear();
                    try {
                        fileChannels[fileNum].read(directBuffers[bufferNum]);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    countDownLatch[fileNum].countDown();
                }
            }

            if(fileIdx >= from && fileIdx < from + directBufferNumber) {
                //如果要读取的在缓存之间，那么直接从缓存中读，顺序读都在这块读
                try {
                    countDownLatch[fileIdx].await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ByteBuffer buffer = directBuffers[fileIdx % directBufferNumber].duplicate();
                buffer.position(startPosition);
                buffer.limit(startPosition + canReadNum * messageLength);
                while (buffer.hasRemaining()) {
                    byte[] msg = bytes[bytePos++];
                    buffer.get(msg);
                    ans.add(msg);
                }
            }
            else {
                //不再缓存区，就直接channel读，随机读一般在这块
                ByteBuffer buffer = bufferPool.poll();//获取到每个线程的拥有的buffer，大小就是10 * 58
                buffer.clear();
                buffer.limit(canReadNum * messageLength);

                FileChannel fileChannel = fileChannelPools[fileIdx].poll();//获取可用的fileChannel，每个文件channel
                try {
                    fileChannel.position(startPosition);
                    fileChannel.read(buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                fileChannelPools[fileIdx].offer(fileChannel);

                buffer.position(0);
                while (buffer.hasRemaining()) {
                    byte[] msg = bytes[bytePos++];
                    buffer.get(msg);
                    ans.add(msg);
                }
                bufferPool.offer(buffer);
            }
            msgNumber -= canReadNum;
            msgOffset += canReadNum;
        }

        return ans;
    }
}