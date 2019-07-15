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
package com.alibaba.dubbo.registry.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.common.utils.UrlUtils;
import com.alibaba.dubbo.registry.NotifyListener;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryFactory;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.cluster.Cluster;
import com.alibaba.dubbo.rpc.cluster.Configurator;
import com.alibaba.dubbo.rpc.protocol.InvokerWrapper;

/**
 * RegistryProtocol
 * 
 * @author william.liangf
 * @author chao.liuc
 */
public class RegistryProtocol implements Protocol {

    private Cluster cluster;
    
    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }
    
    private Protocol protocol;
    
    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    private RegistryFactory registryFactory;
    
    public void setRegistryFactory(RegistryFactory registryFactory) {
        this.registryFactory = registryFactory;
    }

    private ProxyFactory proxyFactory;
    
    public void setProxyFactory(ProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    public int getDefaultPort() {
        return 9090;
    }
    
    private static RegistryProtocol INSTANCE;

    public RegistryProtocol() {
        INSTANCE = this;
    }
    
    public static RegistryProtocol getRegistryProtocol() {
        if (INSTANCE == null) {
            ExtensionLoader.getExtensionLoader(Protocol.class).getExtension(Constants.REGISTRY_PROTOCOL); // load
        }
        return INSTANCE;
    }
    
    private final Map<URL, NotifyListener> overrideListeners = new ConcurrentHashMap<URL, NotifyListener>();
    
    public Map<URL, NotifyListener> getOverrideListeners() {
		return overrideListeners;
	}

	//用于解决rmi重复暴露端口冲突的问题，已经暴露过的服务不再重新暴露
    //providerurl <--> exporter
    private final Map<String, ExporterChangeableWrapper<?>> bounds = new ConcurrentHashMap<String, ExporterChangeableWrapper<?>>();
    
    private final static Logger logger = LoggerFactory.getLogger(RegistryProtocol.class);

    /**
     * 远程导出服务整体实现逻辑：
     * （1）调用doLocalExport导出服务
     * （2）向注册中心注册服务
     * （3）向注册中心订阅 override 数据
     * （4）创建并返回Exporter
     * @param originInvoker
     * @param <T>
     * @return
     * @throws RpcException
     */
    public <T> Exporter<T> export(final Invoker<T> originInvoker) throws RpcException {
        //1. export invoker：暴露服务，Invoker中封装了provider的方法调用，详见前边的getInvoker的分析
        final ExporterChangeableWrapper<T> exporter = doLocalExport(originInvoker);

        //2. registry provider：获取对应注册中心操作对象，比如ZookeeperRegistry
        final Registry registry = getRegistry(originInvoker);
        // 获取要注册到注册中心的地址，eg：
        //dubbo://192.168.0.100:20880/com.kl.dubbotest.provider.export.ProviderExport?anyhost=true&application=dubbo-spi&dubbo=2.5.3&interface=com.kl.dubbotest.provider.export.ProviderExport&methods=providerExport&pid=80235&retries=0&side=provider&timestamp=1562646169549
        final URL registedProviderUrl = getRegistedProviderUrl(originInvoker);
        registry.register(registedProviderUrl); //向注册中心注册服务

        //3. 订阅override数据
        // FIXME 提供者订阅时，会影响同一JVM即暴露服务，又引用同一服务的的场景，因为subscribed以服务名为缓存的key，导致订阅信息覆盖。
        final URL overrideSubscribeUrl = getSubscribedOverrideUrl(registedProviderUrl);
        final OverrideListener overrideSubscribeListener = new OverrideListener(overrideSubscribeUrl);
        overrideListeners.put(overrideSubscribeUrl, overrideSubscribeListener);
        registry.subscribe(overrideSubscribeUrl, overrideSubscribeListener);

        //4. 保证每次export都返回一个新的exporter实例
        return new Exporter<T>() {
            public Invoker<T> getInvoker() {
                return exporter.getInvoker();
            }
            //取消暴露的过程
            public void unexport() {
            	try {
            		exporter.unexport();
            	} catch (Throwable t) {
                	logger.warn(t.getMessage(), t);
                }
                try {
                    //取消注册
                	registry.unregister(registedProviderUrl);
                } catch (Throwable t) {
                	logger.warn(t.getMessage(), t);
                }
                try {
                    //取消订阅
                	overrideListeners.remove(overrideSubscribeUrl);
                	registry.unsubscribe(overrideSubscribeUrl, overrideSubscribeListener);
                } catch (Throwable t) {
                	logger.warn(t.getMessage(), t);
                }
            }
        };
    }

    /**
     * 导出服务整体逻辑：
     * （1）从缓存获取exporter：ExporterChangeableWrapper，存在则返回
     * （2）缓存不存在
     * （3）创建Invoker的委托类对象InvokerDelegete
     * （4）调用DubboProtocol的export方法导出服务，并构建exporter：ExporterChangeableWrapper
     * （5）返回exporter：ExporterChangeableWrapper
     * @param originInvoker
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> ExporterChangeableWrapper<T>  doLocalExport(final Invoker<T> originInvoker){
        //通过originInvoker构造缓存key
        String key = getCacheKey(originInvoker);
        ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null) {
            synchronized (bounds) {
                exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
                if (exporter == null) {
                    //1. 创建Invoker的委托类对象InvokerDelegete
                    //InvokerDelegete是RegistryProtocol类的静态内部类，继承InvokerWrapper，最超类为Invoker接口
                    //URL是通过getProviderUrl(originInvoker)返回的，此时url的协议已是dubbo，即服务暴露的协议
                    final Invoker<?> invokerDelegete = new InvokerDelegete<T>(originInvoker, getProviderUrl(originInvoker));

                    //2. 调用DubboProtocol的export方法导出服务
                    //ExporterChangeableWrapper是RegistryProtocol的私有内部类，实现了Exporter接口
                    //通过调用它的构造方法(Exporter<T> exporter, Invoker<T> originInvoker)构造exporterWrapper实例
                    //而这里传入的exporter是通过(Exporter<T>) protocol.export(invokerDelegete)语句创建
                    //由上一步知道，这里的invokerDelegete里url属性的protocol协议已经是dubbo
                    exporter = new ExporterChangeableWrapper<T>((Exporter<T>)protocol.export(invokerDelegete), originInvoker);
                    bounds.put(key, exporter);
                }
            }
        }
        return (ExporterChangeableWrapper<T>) exporter;
    }

    /*public com.alibaba.dubbo.rpc.Exporter export(com.alibaba.dubbo.rpc.Invoker arg0)
            throws com.alibaba.dubbo.rpc.RpcException {
        if (arg0 == null) {
            throw new IllegalArgumentException("com.alibaba.dubbo.rpc.Invoker argument == null");
        }
        if (arg0.getUrl() == null) {
            throw new IllegalArgumentException(
                    "com.alibaba.dubbo.rpc.Invoker argument getUrl() == null");
        }
        com.alibaba.dubbo.common.URL url = arg0.getUrl();
        String extName = (url.getProtocol() == null ? "dubbo" : url.getProtocol());
        if (extName == null) {
            throw new IllegalStateException(
                    "Fail to get extension(com.alibaba.dubbo.rpc.Protocol) name from url(" + url
                            .toString() + ") use keys([protocol])");
        }
        //由于这里invokerDelegete的url属性的protocol是dubbo，所以这里走DubboProtocol的export协议
        com.alibaba.dubbo.rpc.Protocol extension = (com.alibaba.dubbo.rpc.Protocol) ExtensionLoader
                .getExtensionLoader(com.alibaba.dubbo.rpc.Protocol.class).getExtension(extName);
        return extension.export(arg0);
    }*/


    /**
     * 对修改了url的invoker重新export
     * @param originInvoker
     * @param newInvokerUrl
     */
    @SuppressWarnings("unchecked")
    private <T> void doChangeLocalExport(final Invoker<T> originInvoker, URL newInvokerUrl){
        String key = getCacheKey(originInvoker);
        final ExporterChangeableWrapper<T> exporter = (ExporterChangeableWrapper<T>) bounds.get(key);
        if (exporter == null){
            logger.warn(new IllegalStateException("error state, exporter should not be null"));
            return ;//不存在是异常场景 直接返回 
        } else {
            final Invoker<T> invokerDelegete = new InvokerDelegete<T>(originInvoker, newInvokerUrl);
            exporter.setExporter(protocol.export(invokerDelegete));
        }
    }

    /**
     * 根据invoker的地址获取registry实例
     * (1)如果url的protocol属性为registry，则修改为zookeeper
     *     eg： url由 "registry://127.0.0.1:2181..."  变成  "zookeeper://127.0.0.1:2181..."
     * (2)根据当前注册中心的配置信息，获得一个匹配的注册中心，也就是ZookeeperRegistry
     * @param originInvoker
     * @return
     */
    private Registry getRegistry(final Invoker<?> originInvoker){
        //registry://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService?application=dubbo-spi&dubbo=2.5.3&export=dubbo://192.168.0.100:20880/com.kl.dubbotest.provider.export.ProviderExport?anyhost=true&application=dubbo-spi&dubbo=2.5.3&interface=com.kl.dubbotest.provider.export.ProviderExport&methods=providerExport&pid=90311&retries=0&side=provider&timestamp=1562808825201&pid=90311&registry=zookeeper&timestamp=1562808825142
        URL registryUrl = originInvoker.getUrl();
        //判断协议头protocol是否为registry
        if (Constants.REGISTRY_PROTOCOL.equals(registryUrl.getProtocol())) {
            //得到zookeeper的协议地址：zookeeper
            String protocol = registryUrl.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_DIRECTORY);
            //registryUrl变成  zookeeper://127.0.0.1:2181/com.alibaba.dubbo.registry.RegistryService?application=dubbo-spi&dubbo=2.5.3&export=dubbo://192.168.0.100:20880/com.kl.dubbotest.provider.export.ProviderExport?anyhost=true&application=dubbo-spi&dubbo=2.5.3&interface=com.kl.dubbotest.provider.export.ProviderExport&methods=providerExport&pid=90311&retries=0&side=provider&timestamp=1562808825201&pid=90311&timestamp=1562808825142
            registryUrl = registryUrl.setProtocol(protocol).removeParameter(Constants.REGISTRY_KEY);
        }
        //registryFactory为RegistryFactory$Adaptive
        return registryFactory.getRegistry(registryUrl);
    }


    /*public class RegistryFactory$Adaptive implements com.alibaba.dubbo.registry.RegistryFactory {

        public com.alibaba.dubbo.registry.Registry getRegistry(com.alibaba.dubbo.common.URL arg0) {
            if (arg0 == null) {
                throw new IllegalArgumentException("url == null");
            }
            com.alibaba.dubbo.common.URL url = arg0;
            String extName = (url.getProtocol() == null ? "dubbo" : url.getProtocol());
            if (extName == null) {
                throw new IllegalStateException(
                        "Fail to get extension(com.alibaba.dubbo.registry.RegistryFactory) "
                                + "name from url(" + url.toString() + ") use keys([protocol])");
            }
            //为ZookeeperRegistryFactory
            com.alibaba.dubbo.registry.RegistryFactory extension = (com.alibaba.dubbo.registry.RegistryFactory) ExtensionLoader
                    .getExtensionLoader(com.alibaba.dubbo.registry.RegistryFactory.class).
                            getExtension(extName);
            return extension.getRegistry(arg0);
        }
    }*/

    /**
     * 返回注册到注册中心的URL，对URL参数进行一次过滤
     * @param originInvoker
     * @return
     */
    private URL getRegistedProviderUrl(final Invoker<?> originInvoker){
        URL providerUrl = getProviderUrl(originInvoker);
        //注册中心看到的地址
        final URL registedProviderUrl = providerUrl.removeParameters(getFilteredKeys(providerUrl)).removeParameter(Constants.MONITOR_KEY);
        return registedProviderUrl;
    }
    
    private URL getSubscribedOverrideUrl(URL registedProviderUrl){
    	return registedProviderUrl.setProtocol(Constants.PROVIDER_PROTOCOL)
                .addParameters(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY, 
                        Constants.CHECK_KEY, String.valueOf(false));
    }

    /**
     * 通过invoker的url 获取 providerUrl的地址
     * @param origininvoker
     * @return
     */
    private URL getProviderUrl(final Invoker<?> origininvoker){
        String export = origininvoker.getUrl().getParameterAndDecoded(Constants.EXPORT_KEY);
        if (export == null || export.length() == 0) {
            throw new IllegalArgumentException("The registry export url is null! registry: " + origininvoker.getUrl());
        }
        
        URL providerUrl = URL.valueOf(export);
        return providerUrl;
    }

    /**
     * 获取invoker在bounds中缓存的key
     * @param originInvoker
     * @return
     */
    private String getCacheKey(final Invoker<?> originInvoker){
        URL providerUrl = getProviderUrl(originInvoker);
        String key = providerUrl.removeParameters("dynamic", "enabled").toFullString();
        return key;
    }

    /**
     * 创建Invoker对象
     * （1）url设置协议头protocol，默认为dubbo
     * （2）根据url加载对应的Registry实例
     * （3）从url获取group，根据group决定使用哪个Cluster的实例
     * （4）调用doRefer方法生成Invoker
     * @param type 扩张接口类Class
     * @param url 远程服务的URL地址
     * @param <T>
     * @return
     * @throws RpcException
     */
    @SuppressWarnings("unchecked")
	public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        // 取 registry 参数值，并将其设置为协议头
        url = url.setProtocol(url.getParameter(Constants.REGISTRY_KEY, Constants.DEFAULT_REGISTRY)).removeParameter(Constants.REGISTRY_KEY);
        // 获取注册中心实例
        Registry registry = registryFactory.getRegistry(url);
        if (RegistryService.class.equals(type)) {
        	return proxyFactory.getInvoker((T) registry, type, url);
        }

        // group="a,b" or group="*"  将 url 查询字符串转为 Map
        Map<String, String> qs = StringUtils.parseQueryString(url.getParameterAndDecoded(Constants.REFER_KEY));
        // 获取 group 配置
        String group = qs.get(Constants.GROUP_KEY);
        if (group != null && group.length() > 0 ) {
            if ( ( Constants.COMMA_SPLIT_PATTERN.split( group ) ).length > 1
                    || "*".equals( group ) ) {
                // 通过 SPI 加载 MergeableCluster 实例，并调用 doRefer 继续执行服务引用逻辑
                return doRefer( getMergeableCluster(), registry, type, url );
            }
        }
        // 调用 doRefer 继续执行服务引用逻辑
        return doRefer(cluster, registry, type, url);
    }
    
    private Cluster getMergeableCluster() {
        return ExtensionLoader.getExtensionLoader(Cluster.class).getExtension("mergeable");
    }

    /**
     * 生成Invoker
     * （1）创建RegistryDirectory
     * （2）生成消费者链接URL
     * （3）向注册中心注册：新建zk节点
     * （4）订阅providers、configurators、routers等节点下的数据
     * （5）一个服务会部署在多台机器上，这样就会在providers产生多个节点，就需要Cluster将多个服务节点合并为一个，并生成一个Invoker
     * @param cluster
     * @param registry
     * @param type 扩张接口Class
     * @param url
     * @param <T>
     * @return
     */
    private <T> Invoker<T> doRefer(Cluster cluster, Registry registry, Class<T> type, URL url) {
        // 创建 RegistryDirectory 实例
        RegistryDirectory<T> directory = new RegistryDirectory<T>(type, url);
        // 设置注册中心和协议
        directory.setRegistry(registry);
        directory.setProtocol(protocol);
        // 生成服务消费者链接
        URL subscribeUrl = new URL(Constants.CONSUMER_PROTOCOL, NetUtils.getLocalHost(), 0, type.getName(), directory.getUrl().getParameters());

        // 注册服务消费者，在 consumers 目录下新建节点
        if (! Constants.ANY_VALUE.equals(url.getServiceInterface())
                && url.getParameter(Constants.REGISTER_KEY, true)) {
            registry.register(subscribeUrl.addParameters(Constants.CATEGORY_KEY, Constants.CONSUMERS_CATEGORY,
                    Constants.CHECK_KEY, String.valueOf(false)));
        }

        // 订阅 providers、configurators、routers 等节点数据, RegistryDirectory会收到这几个节点下的子节点信息
        directory.subscribe(subscribeUrl.addParameter(Constants.CATEGORY_KEY, 
                Constants.PROVIDERS_CATEGORY 
                + "," + Constants.CONFIGURATORS_CATEGORY 
                + "," + Constants.ROUTERS_CATEGORY));

        // 一个注册中心可能有多个服务提供者，因此这里需要将多个服务提供者合并为一个
        //一个服务会部署在多台机器上，这样就会在providers产生多个节点，就需要Cluster将多个服务节点合并为一个，并生成一个Invoker
        return cluster.join(directory);
    }

    //过滤URL中不需要输出的参数(以点号开头的)
    private static String[] getFilteredKeys(URL url) {
        Map<String, String> params = url.getParameters();
        if (params != null && !params.isEmpty()) {
            List<String> filteredKeys = new ArrayList<String>();
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry != null && entry.getKey() != null && entry.getKey().startsWith(Constants.HIDE_KEY_PREFIX)) {
                    filteredKeys.add(entry.getKey());
                }
            }
            return filteredKeys.toArray(new String[filteredKeys.size()]);
        } else {
            return new String[] {};
        }
    }
    
    public void destroy() {
        List<Exporter<?>> exporters = new ArrayList<Exporter<?>>(bounds.values());
        for(Exporter<?> exporter :exporters){
            exporter.unexport();
        }
        bounds.clear();
    }
    
    
    /*重新export 1.protocol中的exporter destory问题 
     *1.要求registryprotocol返回的exporter可以正常destroy
     *2.notify后不需要重新向注册中心注册 
     *3.export 方法传入的invoker最好能一直作为exporter的invoker.
     */
    private class OverrideListener implements NotifyListener {
    	
    	private volatile List<Configurator> configurators;
    	
    	private final URL subscribeUrl;

		public OverrideListener(URL subscribeUrl) {
			this.subscribeUrl = subscribeUrl;
		}

		/*
         *  provider 端可识别的override url只有这两种.
         *  override://0.0.0.0/serviceName?timeout=10
         *  override://0.0.0.0/?timeout=10
         */
        public void notify(List<URL> urls) {
        	List<URL> result = null;
        	for (URL url : urls) {
        		URL overrideUrl = url;
        		if (url.getParameter(Constants.CATEGORY_KEY) == null
        				&& Constants.OVERRIDE_PROTOCOL.equals(url.getProtocol())) {
        			// 兼容旧版本
        			overrideUrl = url.addParameter(Constants.CATEGORY_KEY, Constants.CONFIGURATORS_CATEGORY);
        		}
        		if (! UrlUtils.isMatch(subscribeUrl, overrideUrl)) {
        			if (result == null) {
        				result = new ArrayList<URL>(urls);
        			}
        			result.remove(url);
        			logger.warn("Subsribe category=configurator, but notifed non-configurator urls. may be registry bug. unexcepted url: " + url);
        		}
        	}
        	if (result != null) {
        		urls = result;
        	}
        	this.configurators = RegistryDirectory.toConfigurators(urls);
            List<ExporterChangeableWrapper<?>> exporters = new ArrayList<ExporterChangeableWrapper<?>>(bounds.values());
            for (ExporterChangeableWrapper<?> exporter : exporters){
                Invoker<?> invoker = exporter.getOriginInvoker();
                final Invoker<?> originInvoker ;
                if (invoker instanceof InvokerDelegete){
                    originInvoker = ((InvokerDelegete<?>)invoker).getInvoker();
                }else {
                    originInvoker = invoker;
                }
                
                URL originUrl = RegistryProtocol.this.getProviderUrl(originInvoker);
                URL newUrl = getNewInvokerUrl(originUrl, urls);
                
                if (! originUrl.equals(newUrl)){
                    RegistryProtocol.this.doChangeLocalExport(originInvoker, newUrl);
                }
            }
        }
        
        private URL getNewInvokerUrl(URL url, List<URL> urls){
        	List<Configurator> localConfigurators = this.configurators; // local reference
            // 合并override参数
            if (localConfigurators != null && localConfigurators.size() > 0) {
                for (Configurator configurator : localConfigurators) {
                    url = configurator.configure(url);
                }
            }
            return url;
        }
    }
    
    public static class InvokerDelegete<T> extends InvokerWrapper<T>{
        private final Invoker<T> invoker;
        /**
         * @param invoker 
         * @param url invoker.getUrl返回此值
         */
        public InvokerDelegete(Invoker<T> invoker, URL url){
            super(invoker, url);
            this.invoker = invoker;
        }
        public Invoker<T> getInvoker(){
            if (invoker instanceof InvokerDelegete){
                return ((InvokerDelegete<T>)invoker).getInvoker();
            } else {
                return invoker;
            }
        }
    }
    
    /**
     * exporter代理,建立返回的exporter与protocol export出的exporter的对应关系，在override时可以进行关系修改.
     * 
     * @author chao.liuc
     *
     * @param <T>
     */
    private class ExporterChangeableWrapper<T> implements Exporter<T>{
    	
        private Exporter<T> exporter;
        
        private final Invoker<T> originInvoker;

        public ExporterChangeableWrapper(Exporter<T> exporter, Invoker<T> originInvoker){
            this.exporter = exporter;
            this.originInvoker = originInvoker;
        }
        
        public Invoker<T> getOriginInvoker() {
            return originInvoker;
        }

        public Invoker<T> getInvoker() {
            return exporter.getInvoker();
        }
        
        public void setExporter(Exporter<T> exporter){
            this.exporter = exporter;
        }

        public void unexport() {
            String key = getCacheKey(this.originInvoker);
            bounds.remove(key);
            exporter.unexport();
        }
    }
}