package io.openmessaging;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

public class test {
    public static void main(String args[]) throws Exception {

        /*
        *
        int size = 580 * 1024 * 1024;
        FileChannel fileChannel = new RandomAccessFile("222", "rw").getChannel();
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(size);
        long sendStart = System.currentTimeMillis();
        fileChannel.write(directBuffer);
        long sendEnd = System.currentTimeMillis();
        System.out.println(sendEnd - sendStart);
        */

        int num = 200000000;
        Random random = new Random();

        int num1 = random.nextInt(10);
        int num2 = random.nextInt(10);

        int [] a = new int[10000];

        int temp = 0;
        long sendStart = System.currentTimeMillis();
        for (int i =0; i<num;i++){
            //temp = num1 % 4;
            temp = a.length;
        }
        long sendEnd = System.currentTimeMillis();
        System.out.println(sendEnd - sendStart);
        for (int i =0; i<num;i++){
            //temp = num1 & 0x3;
        }
        long sendEnd1 = System.currentTimeMillis();
        System.out.println(sendEnd1 - sendEnd);
    }

    private static void testFun(){
        ByteBuffer[] bfs = new ByteBuffer[100];

        // 申请一些堆外内存总数
        for(int i = 0; i < 1; i++){
            bfs[i] = ByteBuffer.allocateDirect(580 * 1000 * 1000);
            System.out.println("direct buffer NO. " + (i+1) + " allocate finish");
        }

        FileChannel currentChannel;

        // 测试落盘和读盘性能
        Random random = new Random();

        try {
            long writeTimeTotal = 0;
            long start, end;
            System.out.println("start to write disk!");
            for (int i = 0; i < 20; i++){
                currentChannel = new RandomAccessFile(String.valueOf(i), "rw").getChannel();
                byte[] randomMsg = new byte[580 * 1000 * 1000];
                random.nextBytes(randomMsg);
                start = System.currentTimeMillis();
                currentChannel.write(ByteBuffer.wrap(randomMsg));
                end = System.currentTimeMillis();
                writeTimeTotal += (end - start);
                System.out.println(i + " Time write disk :" + (end - start));
                currentChannel.close();
            }
            System.out.println("10 Times write disk :" + writeTimeTotal);

            long readTimeTotal = 0;
            for (int i = 0; i < 20; i++) {
                currentChannel = new RandomAccessFile(String.valueOf(i), "rw").getChannel();
                bfs[0].clear();
                start = System.currentTimeMillis();
                currentChannel.read(bfs[0]);
                end = System.currentTimeMillis();
                readTimeTotal += (end - start);
                System.out.println(i + " Time read disk :" + (end - start));
                currentChannel.close();
            }
            System.out.println("10 Times read disk :" + readTimeTotal);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 再申请一些堆外内存总数
        for(int i = 1; i < 8; i++){
            bfs[i] = ByteBuffer.allocateDirect(580 * 1000 * 1000);
            System.out.println("direct buffer NO. " + (i+1) + " allocate finish");
        }
    }
}
