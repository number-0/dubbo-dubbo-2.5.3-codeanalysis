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
package com.alibaba.dubbo.rpc.cluster.support.wrapper;

import java.util.List;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcInvocation;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.rpc.cluster.Directory;
import com.alibaba.dubbo.rpc.support.MockInvoker;

/**
 * @author chao.liuc
 */
public class MockClusterInvoker<T> implements Invoker<T>{
	
	private static final Logger logger = LoggerFactory.getLogger(MockClusterInvoker.class);

	private final Directory<T> directory ;
	
	private final Invoker<T> invoker;

    public MockClusterInvoker(Directory<T> directory, Invoker<T> invoker) {
       	this.directory = directory;
       	this.invoker = invoker;
    }

	public URL getUrl() {
		return directory.getUrl();
	}

	public boolean isAvailable() {
		return directory.isAvailable();
	}

	public void destroy() {
		this.invoker.destroy();
	}

	public Class<T> getInterface() {
		return directory.getInterface();
	}

	/**
	 * 整体执行流程：
	 * （1）获取mock配置值
	 * 		a. false或值得长度为0：只发起远程调用，不执行mock逻辑
	 * 		b. 以force开头：不发起远程调用，只执行mock逻辑
	 * 		c. 其他情况：发起远程调用，当出现rpc异常的时候，执行mock逻辑(服务降级)
	 * 		发起远程调用：FailoverClusterInvoker#invoke；执行mock逻辑(服务降级)：MockClusterInvoker#doMockInvoke
	 * @param invocation
	 * @return
	 * @throws RpcException
	 */
	public Result invoke(Invocation invocation) throws RpcException {
		Result result = null;

		// 获取 mock 配置值
        String value = directory.getUrl().getMethodParameter(invocation.getMethodName(), Constants.MOCK_KEY, Boolean.FALSE.toString()).trim(); 
        if (value.length() == 0 || value.equalsIgnoreCase("false")){
        	//no mock
			// 无 mock 逻辑，直接调用其他 Invoker 对象的 invoke 方法，
			// 比如 FailoverClusterInvoker
        	result = this.invoker.invoke(invocation);
        } else if (value.startsWith("force")) {
        	if (logger.isWarnEnabled()) {
        		logger.info("force-mock: " + invocation.getMethodName() + " force-mock enabled , url : " +  directory.getUrl());
        	}
        	//force:direct mock
			// force:xxx 直接执行 mock 逻辑，不发起远程调用
        	result = doMockInvoke(invocation, null);
        } else {
        	//fail-mock
			// fail:xxx 表示消费方对调用服务失败后，再执行 mock 逻辑，不抛出异常
        	try {
				// 调用其他 Invoker 对象的 invoke 方法
        		result = this.invoker.invoke(invocation);
        	}catch (RpcException e) {
				if (e.isBiz()) {
					throw e;
				} else {
					if (logger.isWarnEnabled()) {
		        		logger.info("fail-mock: " + invocation.getMethodName() + " fail-mock enabled , url : " +  directory.getUrl(), e);
		        	}
					// 服务降级：调用失败，执行 mock 逻辑
					result = doMockInvoke(invocation, e);
				}
			}
        }
        return result;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Result doMockInvoke(Invocation invocation,RpcException e){
		Result result = null;
    	Invoker<T> minvoker ;
    	
    	List<Invoker<T>> mockInvokers = selectMockInvoker(invocation);
		if (mockInvokers == null || mockInvokers.size() == 0){
			minvoker = (Invoker<T>) new MockInvoker(directory.getUrl());
		} else {
			minvoker = mockInvokers.get(0);
		}
		try {
			result = minvoker.invoke(invocation);
		} catch (RpcException me) {
			if (me.isBiz()) {
				result = new RpcResult(me.getCause());
			} else {
				throw new RpcException(me.getCode(), getMockExceptionMessage(e, me), me.getCause());
			}
//			
		} catch (Throwable me) {
			throw new RpcException(getMockExceptionMessage(e, me), me.getCause());
		}
		return result;
    }
	
	private String getMockExceptionMessage(Throwable t, Throwable mt){
		String msg = "mock error : " + mt.getMessage();
		if (t != null){
			msg = msg + ", invoke error is :" + StringUtils.toString(t);
		}
		return msg;
	}

	/**
     * 返回MockInvoker
     * 契约：
     * directory根据invocation中是否有Constants.INVOCATION_NEED_MOCK，来判断获取的是一个normal invoker 还是一个 mock invoker
     * 如果directorylist 返回多个mock invoker，只使用第一个invoker.
     * @param invocation
     * @return 
     */
    private List<Invoker<T>> selectMockInvoker(Invocation invocation){
    	//TODO generic invoker？
        if (invocation instanceof RpcInvocation){
            //存在隐含契约(虽然在接口声明中增加描述，但扩展性会存在问题.同时放在attachement中的做法需要改进
        	((RpcInvocation)invocation).setAttachment(Constants.INVOCATION_NEED_MOCK, Boolean.TRUE.toString());
            //directory根据invocation中attachment是否有Constants.INVOCATION_NEED_MOCK，来判断获取的是normal invokers or mock invokers
        	List<Invoker<T>> invokers = directory.list(invocation);
            return invokers;
        } else {
            return null ;
        }
    }

	@Override
	public String toString() {
		return "invoker :" + this.invoker + ",directory: " + this.directory;
	}
}