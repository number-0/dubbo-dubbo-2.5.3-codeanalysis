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
package com.alibaba.dubbo.rpc.proxy;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.service.EchoService;

/**
 * AbstractProxyFactory
 * 
 * @author william.liangf
 */
public abstract class AbstractProxyFactory implements ProxyFactory {

    /**
     * 获取服务代理类对象
     * （1）获取接口数组：interfaces
     * （2）调用JavassistProxyFactory#getProxy获取服务代理类对象
     * @param invoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        Class<?>[] interfaces = null;
        // 获取接口列表
        String config = invoker.getUrl().getParameter("interfaces");
        if (config != null && config.length() > 0) {
            // 切分接口列表
            String[] types = Constants.COMMA_SPLIT_PATTERN.split(config);
            if (types != null && types.length > 0) {
                interfaces = new Class<?>[types.length + 2];
                // 设置服务接口类和 EchoService.class 到 interfaces 中
                interfaces[0] = invoker.getInterface();
                interfaces[1] = EchoService.class;
                for (int i = 0; i < types.length; i ++) {
                    // 加载接口类
                    interfaces[i + 1] = ReflectUtils.forName(types[i]);
                }
            }
        }
        if (interfaces == null) {
            interfaces = new Class<?>[] {invoker.getInterface(), EchoService.class};
        }

        // 调用重载方法JavassistProxyFactory#getProxy
        return getProxy(invoker, interfaces);
    }
    
    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}