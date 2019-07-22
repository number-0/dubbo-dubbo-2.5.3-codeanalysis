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
package com.alibaba.dubbo.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.Version;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.dubbo.config.support.Parameter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.StaticContext;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.directory.StaticDirectory;
import com.alibaba.dubbo.rpc.cluster.support.AvailableCluster;
import com.alibaba.dubbo.rpc.cluster.support.ClusterUtils;
import com.alibaba.dubbo.rpc.protocol.injvm.InjvmProtocol;
import com.alibaba.dubbo.rpc.service.GenericService;
import com.alibaba.dubbo.rpc.support.ProtocolUtils;

/**
 * ReferenceConfig
 * 
 * @author william.liangf
 * @export
 */
public class ReferenceConfig<T> extends AbstractReferenceConfig {

    private static final long    serialVersionUID        = -5864351140409987595L;

    private static final Protocol refprotocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();

    private static final Cluster cluster = ExtensionLoader.getExtensionLoader(Cluster.class).getAdaptiveExtension();
    
    private static final ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    // 接口类型
    private String               interfaceName;

    private Class<?>             interfaceClass;

    // 客户端类型
    private String               client;

    // 点对点直连服务提供地址
    private String               url;

    // 方法配置
    private List<MethodConfig>   methods;

    // 缺省配置
    private ConsumerConfig       consumer;
    
    private String				 protocol;

    // 接口代理类引用
    private transient volatile T ref;

    private transient volatile Invoker<?> invoker;

    private transient volatile boolean initialized;

    private transient volatile boolean destroyed;

    private final List<URL> urls = new ArrayList<URL>();

    @SuppressWarnings("unused")
    private final Object finalizerGuardian = new Object() {
        @Override
        protected void finalize() throws Throwable {
            super.finalize();

            if(! ReferenceConfig.this.destroyed) {
                logger.warn("ReferenceConfig(" + url + ") is not DESTROYED when FINALIZE");

                /* 先不做Destroy操作
                try {
                    ReferenceConfig.this.destroy();
                } catch (Throwable t) {
                        logger.warn("Unexpected err when destroy invoker of ReferenceConfig(" + url + ") in finalize method!", t);
                }
                */
            }
        }
    };

    public ReferenceConfig() {}
    
    public ReferenceConfig(Reference reference) {
        appendAnnotation(Reference.class, reference);
    }

    public URL toUrl() {
        return urls == null || urls.size() == 0 ? null : urls.iterator().next();
    }

    public List<URL> toUrls() {
        return urls;
    }

    public synchronized T get() {
        if (destroyed){
            throw new IllegalStateException("Already destroyed!");
        }
        // 检测 ref 是否为空，为空则通过 init 方法创建
    	if (ref == null) {
            // init 方法主要用于处理配置，以及调用 createProxy 生成代理类
    		init();
    	}
    	return ref;
    }
    
    public synchronized void destroy() {
        if (ref == null) {
            return;
        }
        if (destroyed){
            return;
        }
        destroyed = true;
        try {
            invoker.destroy();
        } catch (Throwable t) {
            logger.warn("Unexpected err when destroy invoker of ReferenceConfig(" + url + ").", t);
        }
        invoker = null;
        ref = null;
    }

    /**
     * 整体实现逻辑：
     * （1）配置的校验，比如consumer对象是否存在
     * （2）从System或配置文件中加载与接口名相对应的配置，并将解析结果赋值给url字段，url字段的作用一般是用于点对点调用
     * （3）检查核心配置类是否为空，比如：application、monitor、registries
     * （4）将一些配置添加到map中
     * （5）处理<dubbo:method> MethodConfig 实例
     * （6）创建接口类对应代理类createProxy(map)，并赋值给成员变量ref
     */
    private void init() {
        // 避免重复初始化
	    if (initialized) {
	        return;
	    }
	    initialized = true;
        // 检测接口名合法性
    	if (interfaceName == null || interfaceName.length() == 0) {
    	    throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
    	}

        // 检测<dubbo:consumer>标签对应的consumer变量是否为空，为空则创建
    	checkDefault();
        appendProperties(this);
        if (getGeneric() == null && getConsumer() != null) {
            // 设置 generic
            setGeneric(getConsumer().getGeneric());
        }
        // 检测是否为泛化接口
        if (ProtocolUtils.isGeneric(getGeneric())) {
            interfaceClass = GenericService.class;
        } else {
            try {
                // 加载接口类
				interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
				        .getContextClassLoader());
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e.getMessage(), e);
			}
            checkInterfaceAndMethods(interfaceClass, methods);
        }

        // -------------------------------✨ 分割线1 ✨------------------------------
        //从系统属性或配置文件中加载与接口名相对应的配置，并将解析结果赋值给 url 字段,url 字段的作用一般是用于点对点调用

        // 从系统变量中获取与接口名对应的属性值
        String resolve = System.getProperty(interfaceName);
        String resolveFile = null;
        if (resolve == null || resolve.length() == 0) {
            // 从系统属性中获取解析文件路径
	        resolveFile = System.getProperty("dubbo.resolve.file");
	        if (resolveFile == null || resolveFile.length() == 0) {
                // 从指定位置加载配置文件
	        	File userResolveFile = new File(new File(System.getProperty("user.home")), "dubbo-resolve.properties");
	        	if (userResolveFile.exists()) {
                    // 获取文件绝对路径
	        		resolveFile = userResolveFile.getAbsolutePath();
	        	}
	        }
	        if (resolveFile != null && resolveFile.length() > 0) {
	        	Properties properties = new Properties();
	        	FileInputStream fis = null;
	        	try {
	        	    fis = new FileInputStream(new File(resolveFile));
                    // 从文件中加载配置
					properties.load(fis);
				} catch (IOException e) {
					throw new IllegalStateException("Unload " + resolveFile + ", cause: " + e.getMessage(), e);
				} finally {
				    try {
                        if(null != fis) fis.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
				}
                // 获取与接口名对应的配置
	        	resolve = properties.getProperty(interfaceName);
	        }
        }
        if (resolve != null && resolve.length() > 0) {
            // 将 resolve 赋值给 url
        	url = resolve;
        	if (logger.isWarnEnabled()) {
        		if (resolveFile != null && resolveFile.length() > 0) {
        			logger.warn("Using default dubbo resolve file " + resolveFile + " replace " + interfaceName + "" + resolve + " to p2p invoke remote service.");
        		} else {
        			logger.warn("Using -D" + interfaceName + "=" + resolve + " to p2p invoke remote service.");
        		}
    		}
        }

        // -------------------------------✨ 分割线2 ✨------------------------------
        //检查核心配置类是否为空，比如：application、monitor、registries

        if (consumer != null) {
            if (application == null) {
                // 从 consumer 中获取 Application 实例，下同
                application = consumer.getApplication();
            }
            if (module == null) {
                module = consumer.getModule();
            }
            if (registries == null) {
                registries = consumer.getRegistries();
            }
            if (monitor == null) {
                monitor = consumer.getMonitor();
            }
        }
        if (module != null) {
            if (registries == null) {
                registries = module.getRegistries();
            }
            if (monitor == null) {
                monitor = module.getMonitor();
            }
        }
        if (application != null) {
            if (registries == null) {
                registries = application.getRegistries();
            }
            if (monitor == null) {
                monitor = application.getMonitor();
            }
        }
        // 检测 Application 合法性
        checkApplication();
        // 检测本地存根配置合法性
        checkStubAndMock(interfaceClass);

        // -------------------------------✨ 分割线3 ✨------------------------------
        //将一些配置添加到map中

        Map<String, String> map = new HashMap<String, String>();
        Map<Object, Object> attributes = new HashMap<Object, Object>();

        // 添加 side、协议版本信息、时间戳和进程号等信息到 map 中
        map.put(Constants.SIDE_KEY, Constants.CONSUMER_SIDE);
        map.put(Constants.DUBBO_VERSION_KEY, Version.getVersion());
        map.put(Constants.TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(Constants.PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }

        // 非泛化服务
        if (! isGeneric()) {
            // 获取版本
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put("revision", revision);
            }

            // 获取接口方法列表，并添加到 map 中
            String[] methods = Wrapper.getWrapper(interfaceClass).getMethodNames();
            if(methods.length == 0) {
                logger.warn("NO method found in service interface " + interfaceClass.getName());
                map.put("methods", Constants.ANY_VALUE);
            }
            else {
                map.put("methods", StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }
        map.put(Constants.INTERFACE_KEY, interfaceName);
        // 将 ApplicationConfig、ConsumerConfig、ReferenceConfig 等对象的字段信息添加到 map 中
        appendParameters(map, application);
        appendParameters(map, module);
        appendParameters(map, consumer, Constants.DEFAULT_KEY);
        appendParameters(map, this);

        // -------------------------------✨ 分割线4 ✨------------------------------
        //处理<dubbo:method> MethodConfig 实例

        String prifix = StringUtils.getServiceKey(map);
        if (methods != null && methods.size() > 0) {
            // 遍历 MethodConfig 列表
            for (MethodConfig method : methods) {
                appendParameters(map, method, method.getName());
                String retryKey = method.getName() + ".retry";
                // 检测 map 是否包含 methodName.retry
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        // 添加重试次数配置 methodName.retries
                        map.put(method.getName() + ".retries", "0");
                    }
                }

                // 添加 MethodConfig 中的“属性”字段到 attributes
                // 比如 onreturn、onthrow、oninvoke 等
                appendAttributes(attributes, method, prifix + "." + method.getName());
                checkAndConvertImplicitConfig(method, map, attributes);
            }
        }

        // -------------------------------✨ 分割线5 ✨------------------------------

        //attributes通过系统context进行存储.
        StaticContext.getSystemContext().putAll(attributes);
        // 创建代理类
        ref = createProxy(map);
    }
    
    private static void checkAndConvertImplicitConfig(MethodConfig method, Map<String,String> map, Map<Object,Object> attributes){
      //check config conflict
        if (Boolean.FALSE.equals(method.isReturn()) && (method.getOnreturn() != null || method.getOnthrow() != null)) {
            throw new IllegalStateException("method config error : return attribute must be set true when onreturn or onthrow has been setted.");
        }
        //convert onreturn methodName to Method
        String onReturnMethodKey = StaticContext.getKey(map,method.getName(),Constants.ON_RETURN_METHOD_KEY);
        Object onReturnMethod = attributes.get(onReturnMethodKey);
        if (onReturnMethod != null && onReturnMethod instanceof String){
            attributes.put(onReturnMethodKey, getMethodByName(method.getOnreturn().getClass(), onReturnMethod.toString()));
        }
        //convert onthrow methodName to Method
        String onThrowMethodKey = StaticContext.getKey(map,method.getName(),Constants.ON_THROW_METHOD_KEY);
        Object onThrowMethod = attributes.get(onThrowMethodKey);
        if (onThrowMethod != null && onThrowMethod instanceof String){
            attributes.put(onThrowMethodKey, getMethodByName(method.getOnthrow().getClass(), onThrowMethod.toString()));
        }
        //convert oninvoke methodName to Method
        String onInvokeMethodKey = StaticContext.getKey(map,method.getName(),Constants.ON_INVOKE_METHOD_KEY);
        Object onInvokeMethod = attributes.get(onInvokeMethodKey);
        if (onInvokeMethod != null && onInvokeMethod instanceof String){
            attributes.put(onInvokeMethodKey, getMethodByName(method.getOninvoke().getClass(), onInvokeMethod.toString()));
        }
    }

    private static Method getMethodByName(Class<?> clazz, String methodName){
        try {
            return ReflectUtils.findMethodByMethodName(clazz, methodName);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 创建接口类对应代理对象整体流程：
     * （1）根据scope判断是本地引用还是远程引用
     * （2）本地引用：构建InjvmInvoker实例
     * （3）远程引用
     *      a. 用户在System或dubbo-resolve.properties文件有指定配置(一般都是直连配置)，则使用指定的配置URL
     *      b. 不满足a，则使用dubbo配置文件中的注册中心URL
     *      c. 将url都添加到urls中
     *      d.
     *          urls列表数量为1，则直接通过自适应Protocol扩展类构建Invoker实例
     *          urls列表数量大于1，构建每个url对应的invoker并添加到invokers列表，通过Cluster合并多个Invoker
     *      e. 调用ProxyFactory生成代理类
     *
     * @param map
     * @return
     */
	@SuppressWarnings({ "unchecked", "rawtypes", "deprecation" })
	private T createProxy(Map<String, String> map) {
		URL tmpUrl = new URL("temp", "localhost", 0, map);
		final boolean isJvmRefer;
        if (isInjvm() == null) {
            if (url != null && url.length() > 0) { //指定URL的情况下，不做本地引用
                isJvmRefer = false;

            }
            // 根据 url 的协议、scope 以及 injvm 等参数检测是否需要本地引用
            // 比如如果用户显式配置了 scope=local，此时 isInjvmRefer 返回 true
            else if (InjvmProtocol.getInjvmProtocol().isInjvmRefer(tmpUrl)) {
                //默认情况下如果本地有服务暴露，则引用本地服务.
                isJvmRefer = true;
            } else {
                isJvmRefer = false;
            }
        } else {
            // 获取 injvm 配置值
            isJvmRefer = isInjvm().booleanValue();
        }

        // 本地引用
		if (isJvmRefer) {
            // 生成本地引用 URL，协议为 injvm
			URL url = new URL(Constants.LOCAL_PROTOCOL, NetUtils.LOCALHOST, 0, interfaceClass.getName()).addParameters(map);
            // 调用 refer 方法构建 InjvmInvoker 实例
			invoker = refprotocol.refer(interfaceClass, url);
            if (logger.isInfoEnabled()) {
                logger.info("Using injvm service " + interfaceClass.getName());
            }
		}
        // 远程引用
		else {
            if (url != null && url.length() > 0) { // 用户指定URL，指定的URL可能是点对点直连地址，也可能是注册中心URL
                // 当需要配置多个 url 时，用分号进行分割，这里会进行切分
                String[] us = Constants.SEMICOLON_SPLIT_PATTERN.split(url);
                if (us != null && us.length > 0) {
                    for (String u : us) {
                        URL url = URL.valueOf(u);
                        if (url.getPath() == null || url.getPath().length() == 0) {
                            // 设置接口全限定名为 url 路径
                            url = url.setPath(interfaceName);
                        }

                        // 检测 url 协议是否为 registry，若是，表明用户想使用指定的注册中心
                        if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                            // 将 map 转换为查询字符串，并作为 refer 参数的值添加到 url 中
                            urls.add(url.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                        } else {
                            // 合并 url，移除服务提供者的一些配置（这些配置来源于用户配置的 url 属性），
                            // 比如线程池相关配置。并保留服务提供者的部分配置，比如版本，group，时间戳等
                            // 最后将合并后的配置设置为 url 查询字符串中。
                            urls.add(ClusterUtils.mergeUrl(url, map));
                        }
                    }
                }
            } else { // 通过注册中心配置拼装URL
                //加载获取注册中心url
            	List<URL> us = loadRegistries(false);
            	if (us != null && us.size() > 0) {
                	for (URL u : us) {
                	    URL monitorUrl = loadMonitor(u);
                        if (monitorUrl != null) {
                            map.put(Constants.MONITOR_KEY, URL.encode(monitorUrl.toFullString()));
                        }
                        // 添加 refer 参数到 url 中，并将 url 添加到 urls 中
                	    urls.add(u.addParameterAndEncoded(Constants.REFER_KEY, StringUtils.toQueryString(map)));
                    }
            	}

                // 未配置注册中心，抛出异常
            	if (urls == null || urls.size() == 0) {
                    throw new IllegalStateException("No such any registry to reference " + interfaceName  + " on the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() + ", please config <dubbo:registry address=\"...\" /> to your spring config.");
                }
            }

            // 单个注册中心或服务提供者(服务直连，下同)
            if (urls.size() == 1) {
                invoker = refprotocol.refer(interfaceClass, urls.get(0));
            }
            // 多个注册中心或多个服务提供者，或者两者混合
            else {
                List<Invoker<?>> invokers = new ArrayList<Invoker<?>>();
                URL registryURL = null;

                // 获取所有的 Invoker
                for (URL url : urls) {
                    // 通过 refprotocol 调用 refer 构建 Invoker，refprotocol 会在运行时
                    // 根据 url 协议头加载指定的 Protocol 实例，并调用实例的 refer 方法
                    invokers.add(refprotocol.refer(interfaceClass, url));
                    if (Constants.REGISTRY_PROTOCOL.equals(url.getProtocol())) {
                        registryURL = url; // 用了最后一个registry url
                    }
                }
                if (registryURL != null) { // 有 注册中心协议的URL
                    // 对有注册中心的Cluster 只用 AvailableCluster
                    URL u = registryURL.addParameter(Constants.CLUSTER_KEY, AvailableCluster.NAME);
                    // 创建 StaticDirectory 实例，并由 Cluster 对多个 Invoker 进行合并
                    invoker = cluster.join(new StaticDirectory(u, invokers));
                }  else { // 不是 注册中心的URL
                    invoker = cluster.join(new StaticDirectory(invokers));
                }
            }
        }

        Boolean c = check;
        if (c == null && consumer != null) {
            c = consumer.isCheck();
        }
        if (c == null) {
            c = true; // default true
        }

        // invoker 可用性检查
        if (c && ! invoker.isAvailable()) {
            throw new IllegalStateException("Failed to check the status of the service " + interfaceName + ". No provider available for the service " + (group == null ? "" : group + "/") + interfaceName + (version == null ? "" : ":" + version) + " from the url " + invoker.getUrl() + " to the consumer " + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
        }
        if (logger.isInfoEnabled()) {
            logger.info("Refer dubbo service " + interfaceClass.getName() + " from url " + invoker.getUrl());
        }
        // 创建服务代理类
        return (T) proxyFactory.getProxy(invoker);
    }

    private void checkDefault() {
        if (consumer == null) {
            consumer = new ConsumerConfig();
        }
        appendProperties(consumer);
    }

	public Class<?> getInterfaceClass() {
	    if (interfaceClass != null) {
	        return interfaceClass;
	    }
	    if (isGeneric()
            || (getConsumer() != null && getConsumer().isGeneric())) {
	        return GenericService.class;
	    }
	    try {
	        if (interfaceName != null && interfaceName.length() > 0) {
	            this.interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                    .getContextClassLoader());
	        }
        } catch (ClassNotFoundException t) {
            throw new IllegalStateException(t.getMessage(), t);
        }
	    return interfaceClass;
	}
	
	/**
	 * @deprecated
	 * @see #setInterface(Class)
	 * @param interfaceClass
	 */
	@Deprecated
	public void setInterfaceClass(Class<?> interfaceClass) {
	    setInterface(interfaceClass);
	}

    public String getInterface() {
        return interfaceName;
    }

    public void setInterface(String interfaceName) {
        this.interfaceName = interfaceName;
        if (id == null || id.length() == 0) {
            id = interfaceName;
        }
    }
    
    public void setInterface(Class<?> interfaceClass) {
        if (interfaceClass != null && ! interfaceClass.isInterface()) {
            throw new IllegalStateException("The interface class " + interfaceClass + " is not a interface!");
        }
        this.interfaceClass = interfaceClass;
        setInterface(interfaceClass == null ? (String) null : interfaceClass.getName());
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        checkName("client", client);
        this.client = client;
    }

    @Parameter(excluded = true)
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<MethodConfig> getMethods() {
        return methods;
    }

    @SuppressWarnings("unchecked")
    public void setMethods(List<? extends MethodConfig> methods) {
        this.methods = (List<MethodConfig>) methods;
    }

    public ConsumerConfig getConsumer() {
        return consumer;
    }

    public void setConsumer(ConsumerConfig consumer) {
        this.consumer = consumer;
    }

    public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	// just for test
    Invoker<?> getInvoker() {
        return invoker;
    }
    
}