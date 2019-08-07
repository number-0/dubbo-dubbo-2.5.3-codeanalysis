/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.exchange.support;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.Channel;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.TimeoutException;
import com.alibaba.dubbo.remoting.exchange.Request;
import com.alibaba.dubbo.remoting.exchange.Response;
import com.alibaba.dubbo.remoting.exchange.ResponseCallback;
import com.alibaba.dubbo.remoting.exchange.ResponseFuture;

/**
 * DefaultFuture.
 * 
 * @author qian.lei
 * @author chao.liuc
 */
public class DefaultFuture implements ResponseFuture {

    private static final Logger                   logger = LoggerFactory.getLogger(DefaultFuture.class);

    //key:requestId val:Channel
    private static final Map<Long, Channel>       CHANNELS   = new ConcurrentHashMap<Long, Channel>();

    //key:requestId val:DefaultFuture
    //requestId生成见Request#newId：增长到MAX_VALUE时，再增长会变为MIN_VALUE，负数也可以做为ID
    private static final Map<Long, DefaultFuture> FUTURES   = new ConcurrentHashMap<Long, DefaultFuture>();

    // invoke id.
    private final long                            id;

    private final Channel                         channel;
    
    private final Request                         request;

    private final int                             timeout;

    private final Lock                            lock = new ReentrantLock();

    private final Condition                       done = lock.newCondition();

    private final long                            start = System.currentTimeMillis();

    private volatile long                         sent;
    
    private volatile Response                     response;

    private volatile ResponseCallback             callback;

    public DefaultFuture(Channel channel, Request request, int timeout){
        this.channel = channel;
        this.request = request;

        // 获取请求 id，这个 id 很重要，后面还会见到
        this.id = request.getId();
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
        // put into waiting map.
        // 存储 <requestId, DefaultFuture> 映射关系到 FUTURES 中
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }
    
    public Object get() throws RemotingException {
        return get(timeout);
    }

    public Object get(int timeout) throws RemotingException {
        if (timeout <= 0) {
            timeout = Constants.DEFAULT_TIMEOUT;
        }

        // 检测服务提供方是否成功返回了调用结果
        if (! isDone()) {
            long start = System.currentTimeMillis();
            lock.lock();
            try {
                // 循环检测服务提供方是否成功返回了调用结果
                while (! isDone()) {
                    // 如果调用结果尚未返回，这里等待一段时间
                    done.await(timeout, TimeUnit.MILLISECONDS);
                    // 如果调用结果成功返回，或等待超时，此时跳出 while 循环，执行后续的逻辑
                    if (isDone() || System.currentTimeMillis() - start > timeout) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }

            // 如果调用结果仍未返回，则抛出超时异常
            if (! isDone()) {
                throw new TimeoutException(sent > 0, channel, getTimeoutMessage(false));
            }
        }

        // 返回调用结果
        return returnFromResponse();
    }
    
    public void cancel(){
        Response errorResult = new Response(id);
        errorResult.setErrorMessage("request future has been canceled.");
        response = errorResult ;
        FUTURES.remove(id);
        CHANNELS.remove(id);
    }

    public boolean isDone() {
        // 通过检测 response 字段为空与否，判断是否收到了调用结果
        return response != null;
    }

    public void setCallback(ResponseCallback callback) {
        if (isDone()) {
            invokeCallback(callback);
        } else {
            boolean isdone = false;
            lock.lock();
            try{
                if (!isDone()) {
                    this.callback = callback;
                } else {
                    isdone = true;
                }
            }finally {
                lock.unlock();
            }
            if (isdone){
                invokeCallback(callback);
            }
        }
    }
    private void invokeCallback(ResponseCallback c){
        ResponseCallback callbackCopy = c;
        if (callbackCopy == null){
            throw new NullPointerException("callback cannot be null.");
        }
        c = null;
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null. url:"+channel.getUrl());
        }
        
        if (res.getStatus() == Response.OK) {
            try {
                callbackCopy.done(res.getResult());
            } catch (Exception e) {
                logger.error("callback invoke error .reasult:" + res.getResult() + ",url:" + channel.getUrl(), e);
            }
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            try {
                TimeoutException te = new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
                callbackCopy.caught(te);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        } else {
            try {
                RuntimeException re = new RuntimeException(res.getErrorMessage());
                callbackCopy.caught(re);
            } catch (Exception e) {
                logger.error("callback invoke error ,url:" + channel.getUrl(), e);
            }
        }
    }

    private Object returnFromResponse() throws RemotingException {
        Response res = response;
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }

        // 如果调用结果的状态为 Response.OK，则表示调用过程正常，服务提供方成功返回了调用结果
        if (res.getStatus() == Response.OK) {
            return res.getResult();
        }

        // 抛出异常
        if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {
            throw new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage());
        }
        throw new RemotingException(channel, res.getErrorMessage());
    }

    private long getId() {
        return id;
    }
    
    private Channel getChannel() {
        return channel;
    }
    
    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    private long getStartTimestamp() {
        return start;
    }

    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    private void doSent() {
        sent = System.currentTimeMillis();
    }

    public static void received(Channel channel, Response response) {
        try {
            // 根据调用编号从 FUTURES 集合中查找指定的 DefaultFuture 对象，responseId就是requestId
            DefaultFuture future = FUTURES.remove(response.getId());
            if (future != null) {
                // 继续向下调用
                future.doReceived(response);
            } else {
                logger.warn("The timeout response finally returned at " 
                            + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) 
                            + ", response " + response 
                            + (channel == null ? "" : ", channel: " + channel.getLocalAddress() 
                                + " -> " + channel.getRemoteAddress()));
            }
        } finally {
            CHANNELS.remove(response.getId());
        }
    }

    private void doReceived(Response res) {
        lock.lock();
        try {
            // 保存响应对象
            response = res;
            if (done != null) {
                // 唤醒用户线程
                done.signal();
            }
        } finally {
            lock.unlock();
        }
        if (callback != null) {
            invokeCallback(callback);
        }
    }

    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                    + (scan ? " by scan timer" : "") + ". start time: " 
                    + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: " 
                    + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())) + ","
                    + (sent > 0 ? " client elapsed: " + (sent - start) 
                        + " ms, server elapsed: " + (nowTimestamp - sent)
                        : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                    + timeout + " ms, request: " + request + ", channel: " + channel.getLocalAddress()
                    + " -> " + channel.getRemoteAddress();
    }

    private static class RemotingInvocationTimeoutScan implements Runnable {

        public void run() {
            while (true) {
                try {
                    for (DefaultFuture future : FUTURES.values()) {
                        if (future == null || future.isDone()) {
                            continue;
                        }
                        if (System.currentTimeMillis() - future.getStartTimestamp() > future.getTimeout()) {
                            // create exception response.
                            Response timeoutResponse = new Response(future.getId());
                            // set timeout status.
                            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);
                            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));
                            // handle response.
                            DefaultFuture.received(future.getChannel(), timeoutResponse);
                        }
                    }
                    Thread.sleep(30);
                } catch (Throwable e) {
                    logger.error("Exception when scan the timeout invocation of remoting.", e);
                }
            }
        }
    }

    static {
        Thread th = new Thread(new RemotingInvocationTimeoutScan(), "DubboResponseTimeoutScanTimer");
        th.setDaemon(true);
        th.start();
    }

}