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
package com.alibaba.dubbo.remoting.transport.netty;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.netty.channel.ChannelFuture;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.remoting.ChannelHandler;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.transport.AbstractChannel;

/**
 * NettyChannel.
 * 
 * @author qian.lei
 * @author william.liangf
 */
final class NettyChannel extends AbstractChannel {

    private static final Logger logger = LoggerFactory.getLogger(NettyChannel.class);

    private static final ConcurrentMap<org.jboss.netty.channel.Channel, NettyChannel> channelMap = new ConcurrentHashMap<org.jboss.netty.channel.Channel, NettyChannel>();

    private final org.jboss.netty.channel.Channel channel;

    private final Map<String, Object> attributes = new ConcurrentHashMap<String, Object>();

    /** 私有构造方法 */
    private NettyChannel(org.jboss.netty.channel.Channel channel, URL url, ChannelHandler handler){
        super(url, handler);
        if (channel == null) {
            throw new IllegalArgumentException("netty channel == null;");
        }
        this.channel = channel;
    }

    static NettyChannel getOrAddChannel(org.jboss.netty.channel.Channel ch, URL url, ChannelHandler handler) {
        if (ch == null) {
            return null;
        }

        //返回NettyChannel类
        // 尝试从集合中获取 NettyChannel 实例
        NettyChannel ret = channelMap.get(ch);
        if (ret == null) {
            // 如果 ret = null，则创建一个新的 NettyChannel 实例
            NettyChannel nc = new NettyChannel(ch, url, handler);
            if (ch.isConnected()) {
                // 将 <Channel, NettyChannel> 键值对存入 channelMap 集合中
                ret = channelMap.putIfAbsent(ch, nc);
            }
            if (ret == null) {
                ret = nc;
            }
        }
        return ret;
    }

    static void removeChannelIfDisconnected(org.jboss.netty.channel.Channel ch) {
        if (ch != null && ! ch.isConnected()) {
            channelMap.remove(ch);
        }
    }

    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) channel.getLocalAddress();
    }

    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.getRemoteAddress();
    }

    public boolean isConnected() {
        return channel.isConnected();
    }

    public void send(Object message, boolean sent) throws RemotingException {
        super.send(message, sent);
        
        boolean success = true;
        int timeout = 0;
        try {
            // 发送消息(包含请求和响应消息)
            ChannelFuture future = channel.write(message);

            // sent 的值源于 <dubbo:method sent="true/false" /> 中 sent 的配置值，有两种配置值：
            //   1. true: 等待消息发出，消息发送失败将抛出异常
            //   2. false: 不等待消息发出，将消息放入 IO 队列，即刻返回
            // 默认情况下 sent = false；
            if (sent) {
                timeout = getUrl().getPositiveParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT);
                // 等待消息发出，若在规定时间没能发出，success 会被置为 false
                success = future.await(timeout);
            }
            Throwable cause = future.getCause();
            if (cause != null) {
                throw cause;
            }
        } catch (Throwable e) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress() + ", cause: " + e.getMessage(), e);
        }

        // 若 success 为 false，这里抛出异常
        if(! success) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress()
                    + "in timeout(" + timeout + "ms) limit");
        }
    }

    public void close() {
        try {
            super.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            removeChannelIfDisconnected(channel);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            attributes.clear();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close netty channel " + channel);
            }
            channel.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }
    
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        if (value == null) { // The null value unallowed in the ConcurrentHashMap.
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((channel == null) ? 0 : channel.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        NettyChannel other = (NettyChannel) obj;
        if (channel == null) {
            if (other.channel != null) return false;
        } else if (!channel.equals(other.channel)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "NettyChannel [channel=" + channel + "]";
    }

}