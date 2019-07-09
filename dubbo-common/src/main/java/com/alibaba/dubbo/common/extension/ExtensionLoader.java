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
package com.alibaba.dubbo.common.extension;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.support.ActivateComparator;
import com.alibaba.dubbo.common.logger.Logger;
import com.alibaba.dubbo.common.logger.LoggerFactory;
import com.alibaba.dubbo.common.utils.ConcurrentHashSet;
import com.alibaba.dubbo.common.utils.ConfigUtils;
import com.alibaba.dubbo.common.utils.Holder;
import com.alibaba.dubbo.common.utils.StringUtils;

/**
 * Dubbo使用的扩展点获取。<p>
 * <ul>
 * <li>自动注入关联扩展点。</li>
 * <li>自动Wrap上扩展点的Wrap类。</li>
 * <li>缺省获得的的扩展点是一个Adaptive Instance。
 * </ul>
 * 
 * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">JDK5.0的自动发现机制实现</a>
 * 
 * @author william.liangf
 * @author ding.lid
 *
 * @see com.alibaba.dubbo.common.extension.SPI
 * @see com.alibaba.dubbo.common.extension.Adaptive
 * @see com.alibaba.dubbo.common.extension.Activate
 */
public class ExtensionLoader<T> {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);
    
    private static final String SERVICES_DIRECTORY = "META-INF/services/";

    private static final String DUBBO_DIRECTORY = "META-INF/dubbo/";
    
    private static final String DUBBO_INTERNAL_DIRECTORY = DUBBO_DIRECTORY + "internal/";

    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");

    //key:扩展接口Class val:ExtensionLoader
    private static final ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS = new ConcurrentHashMap<Class<?>, ExtensionLoader<?>>();

    //key: 扩展实现类的Class，val：Class对应的实例
    private static final ConcurrentMap<Class<?>, Object> EXTENSION_INSTANCES = new ConcurrentHashMap<Class<?>, Object>();

    // ==============================

    //一个扩展接口type对应一个ExtensionLoader
    private final Class<?> type;

    private final ExtensionFactory objectFactory;

    // key：扩展实现类Class, val：配置文件中的key
    private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<Class<?>, String>();

    //key：扩展配置文件中配置的key，val：扩展配置文件中配置的val，即扩展实现类全类名
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<Map<String,Class<?>>>();

    //扩展实现类有@Activate注解，需要缓存到map，key：配置文件的key，val：Activate
    private final Map<String, Activate> cachedActivates = new ConcurrentHashMap<String, Activate>();

    //把有@Adaptive注解扩展实现类, 赋给cachedAdaptiveClass，一个type只能有一个扩展实现类有@Adaptive，否则会抛异常
    private volatile Class<?> cachedAdaptiveClass = null;

    //key：配置文件的key，val：Holder中持有扩展实现类的实例
    private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<String, Holder<Object>>();

    //@SPI注解上，@SPI(value="xxx")，xxx代表扩展接口的默认扩展实现类是谁，cachedDefaultName=xxx
    private String cachedDefaultName;

    //cachedAdaptiveInstance持有@Adaptive标记的扩展实现类实例
    private final Holder<Object> cachedAdaptiveInstance = new Holder<Object>();
    private volatile Throwable createAdaptiveInstanceError;

    //扩展接口type的所有包装类，存放到Set中
    private Set<Class<?>> cachedWrapperClasses;
    
    private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<String, IllegalStateException>();
    
    private static <T> boolean withExtensionAnnotation(Class<T> type) {
        return type.isAnnotationPresent(SPI.class);
    }

    /**
     * 根据扩展接口获取ExtensionLoader，一个扩充接口只会对应一个ExtensionLoader
     * 整体逻辑：
     * （1）校验扩展接口是否有@SPI注解
     * （1）从缓存中获取与拓展接口对应的ExtensionLoader
     * （2）若缓存未命中，则创建一个新的实例，并置入缓存
     *  缓存：ConcurrentMap<Class<?>, ExtensionLoader<?>> EXTENSION_LOADERS
     * @param type
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("Extension type == null");
        //校验type是否为接口，dubbo中所有的扩展接口必须加@SPI注解
        if(!type.isInterface()) {
            throw new IllegalArgumentException("Extension type(" + type + ") is not interface!");
        }

        //校验type是否有@SPI注解
        if(!withExtensionAnnotation(type)) {
            throw new IllegalArgumentException("Extension type(" + type + 
                    ") is not extension, because WITHOUT @" + SPI.class.getSimpleName() + " Annotation!");
        }

        ExtensionLoader<T> loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        if (loader == null) {
            EXTENSION_LOADERS.putIfAbsent(type, new ExtensionLoader<T>(type));
            loader = (ExtensionLoader<T>) EXTENSION_LOADERS.get(type);
        }
        return loader;
    }

    private ExtensionLoader(Class<?> type) {
        this.type = type;
        //类型是ExtensionFactory则(ExtensionFactory objectFactory)为null，否则为ExtensionFactory的适配类型实现
        objectFactory = (type == ExtensionFactory.class ? null : ExtensionLoader.getExtensionLoader(ExtensionFactory.class).getAdaptiveExtension());
    }
    
    public String getExtensionName(T extensionInstance) {
        return getExtensionName(extensionInstance.getClass());
    }

    public String getExtensionName(Class<?> extensionClass) {
        return cachedNames.get(extensionClass);
    }

    /**
     * This is equivalent to <pre>
     *     getActivateExtension(url, key, null);
     * </pre>
     *
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @return extension list which are activated.
     * @see #getActivateExtension(com.alibaba.dubbo.common.URL, String, String)
     */
    public List<T> getActivateExtension(URL url, String key) {
        return getActivateExtension(url, key, null);
    }

    /**
     * This is equivalent to <pre>
     *     getActivateExtension(url, values, null);
     * </pre>
     *
     * @see #getActivateExtension(com.alibaba.dubbo.common.URL, String[], String)
     * @param url url
     * @param values extension point names
     * @return extension list which are activated
     */
    public List<T> getActivateExtension(URL url, String[] values) {
        return getActivateExtension(url, values, null);
    }

    /**
     * This is equivalent to <pre>
     *     getActivateExtension(url, url.getParameter(key).split(","), null);
     * </pre>
     *
     * @see #getActivateExtension(com.alibaba.dubbo.common.URL, String[], String)
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @param group group
     * @return extension list which are activated.
     */
    public List<T> getActivateExtension(URL url, String key, String group) {
        String value = url.getParameter(key);
        return getActivateExtension(url, value == null || value.length() == 0 ? null : Constants.COMMA_SPLIT_PATTERN.split(value), group);
    }

    /**
     *
     * getActivateExtension其他方法都走的此方法：获取@Activate注解扩展实现，即可自动激活的实现
     *
     * Get activate extensions.
     * @see com.alibaba.dubbo.common.extension.Activate
     * @param url url
     * @param values extension point names
     * @param group group
     * @return extension list which are activated
     */
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        List<T> exts = new ArrayList<T>();

        //解析配置要使用的名称
        List<String> names = values == null ? new ArrayList<String>(0) : Arrays.asList(values);
        // 如果names不包含"-default",则加载所有@Activates扩展(names指定的扩展)
        if (! names.contains(Constants.REMOVE_VALUE_PREFIX + Constants.DEFAULT_KEY)) {
            getExtensionClasses();// 加载当前Extension所有实现,会获取到当前Extension中所有@Active实现，赋值给cachedActivates变量
            for (Map.Entry<String, Activate> entry : cachedActivates.entrySet()) { // 遍历当前扩展所有的@Activate扩展
                String name = entry.getKey();
                Activate activate = entry.getValue();
                if (isMatchGroup(group, activate.group())) {// 判断group与@Activate的group是否一致
                    T ext = getExtension(name); // 获取扩展示例
                    // 排除names指定的扩展;并且如果names中没有指定移除该扩展(-name)，且当前url匹配结果显示可激活才进行使用
                    if (! names.contains(name)
                            && ! names.contains(Constants.REMOVE_VALUE_PREFIX + name) 
                            && isActive(activate, url)) {
                        exts.add(ext);
                    }
                }
            }
            Collections.sort(exts, ActivateComparator.COMPARATOR);// 默认排序
        }

        // 对names指定的扩展进行专门的处理
        List<T> usrs = new ArrayList<T>();
        for (int i = 0; i < names.size(); i ++) {// 遍历names指定的扩展名
        	String name = names.get(i);
            if (! name.startsWith(Constants.REMOVE_VALUE_PREFIX)
            		&& ! names.contains(Constants.REMOVE_VALUE_PREFIX + name)) {// 未设置移除该扩展
            	if (Constants.DEFAULT_KEY.equals(name)) {// default表示上面已经加载并且排序的exts,将排在default之前的Activate扩展放置到default组之前,例如:ext1,default,ext2
            		if (usrs.size() > 0) {// 如果此时user不为空,则user中存放的是配置在default之前的Activate扩展
	            		exts.addAll(0, usrs); // 注意index是0，放在default前面
	            		usrs.clear();// 放到default之前，然后清空
            		}
            	} else {
	            	T ext = getExtension(name);
	            	usrs.add(ext);
            	}
            }
        }
        if (usrs.size() > 0) {// 这里留下的都是配置在default之后的
        	exts.addAll(usrs);// 添加到default排序之后
        }
        return exts;
    }
    
    private boolean isMatchGroup(String group, String[] groups) {
        if (group == null || group.length() == 0) {
            return true;
        }
        if (groups != null && groups.length > 0) {
            for (String g : groups) {
                if (group.equals(g)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean isActive(Activate activate, URL url) {
        String[] keys = activate.value();
        if (keys == null || keys.length == 0) {
            return true;
        }
        for (String key : keys) {
            for (Map.Entry<String, String> entry : url.getParameters().entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                if ((k.equals(key) || k.endsWith("." + key))
                        && ConfigUtils.isNotEmpty(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 返回扩展点实例，如果没有指定的扩展点或是还没加载（即实例化）则返回<code>null</code>。注意：此方法不会触发扩展点的加载。
     * <p />
     * 一般应该调用{@link #getExtension(String)}方法获得扩展，这个方法会触发扩展点加载。
     *
     * @see #getExtension(String)
     */
    @SuppressWarnings("unchecked")
    public T getLoadedExtension(String name) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Extension name == null");
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            cachedInstances.putIfAbsent(name, new Holder<Object>());
            holder = cachedInstances.get(name);
        }
        return (T) holder.get();
    }

    /**
     * 返回已经加载的扩展点的名字。
     * <p />
     * 一般应该调用{@link #getSupportedExtensions()}方法获得扩展，这个方法会返回所有的扩展点。
     *
     * @see #getSupportedExtensions()
     */
    public Set<String> getLoadedExtensions() {
        return Collections.unmodifiableSet(new TreeSet<String>(cachedInstances.keySet()));
    }

    /**
     * 返回指定名字的扩展实现类实例。如果指定名字的扩展不存在，则抛异常 {@link IllegalStateException}.
     * 整体逻辑：
     * 1. name为true，则getDefaultExtension获取默认实现类，即:SPI注解的value属性会作为name
     *      eg: @SPI(value="xxx")，xxx代表扩展接口的默认扩展实现类是谁，name=xxx
     *      eg：Protocol的默认实现类为DubboProtocol，因为@SPI(value="dubbo")
     * 2. 从缓存cachedInstances中获取扩展实现类实例
     * 3. 缓存为命中，则创建实例，并置入缓存
     * @param name
     * @return
     */
	@SuppressWarnings("unchecked")
	public T getExtension(String name) {
		if (name == null || name.length() == 0)
		    throw new IllegalArgumentException("Extension name == null");
		//如果名称是true，则getDefaultExtension获取默认实现类，即:SPI注解的value属性会作为name
        //eg: @SPI(value="xxx")，xxx代表扩展接口的默认扩展实现类是谁，name=xxx
		if ("true".equals(name)) {
		    return getDefaultExtension();
		}

		//从缓存cachedInstances中获取扩展实现类实例的holder
		Holder<Object> holder = cachedInstances.get(name);
		if (holder == null) {
		    cachedInstances.putIfAbsent(name, new Holder<Object>());
		    holder = cachedInstances.get(name);
		}
		Object instance = holder.get();
		if (instance == null) {
		    synchronized (holder) {
	            instance = holder.get();
	            if (instance == null) {
	                //不存在实例，根据name创建并置入到holder中
	                instance = createExtension(name);
	                holder.set(instance);
	            }
	        }
		}
		return (T) instance;
	}
	
	/**
	 * 返回缺省的扩展，如果没有设置则返回<code>null</code>。 
	 */
	public T getDefaultExtension() {
	    getExtensionClasses();
        if(null == cachedDefaultName || cachedDefaultName.length() == 0
                || "true".equals(cachedDefaultName)) {
            return null;
        }
        return getExtension(cachedDefaultName);
	}

	public boolean hasExtension(String name) {
	    if (name == null || name.length() == 0)
	        throw new IllegalArgumentException("Extension name == null");
	    try {
	        return getExtensionClass(name) != null;
	    } catch (Throwable t) {
	        return false;
	    }
	}
    
	public Set<String> getSupportedExtensions() {
        Map<String, Class<?>> clazzes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<String>(clazzes.keySet()));
    }
    
	/**
	 * 返回缺省的扩展点名，如果没有设置缺省则返回<code>null</code>。 
	 */
	public String getDefaultExtensionName() {
	    getExtensionClasses();
	    return cachedDefaultName;
	}

    /**
     * 编程方式添加新扩展点。
     *
     * @param name 扩展点名
     * @param clazz 扩展点类
     * @throws IllegalStateException 要添加扩展点名已经存在。
     */
    public void addExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if(!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + "not implement Extension " + type);
        }
        if(clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + "can not be interface!");
        }

        if(!clazz.isAnnotationPresent(Adaptive.class)) {
            if(StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if(cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " already existed(Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
        }
        else {
            if(cachedAdaptiveClass != null) {
                throw new IllegalStateException("Adaptive Extension already existed(Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
        }
    }

    /**
     * 编程方式添加替换已有扩展点。
     *
     * @param name 扩展点名
     * @param clazz 扩展点类
     * @throws IllegalStateException 要添加扩展点名已经存在。
     * @deprecated 不推荐应用使用，一般只在测试时可以使用
     */
    @Deprecated
    public void replaceExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if(!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                    clazz + "not implement Extension " + type);
        }
        if(clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                    clazz + "can not be interface!");
        }

        if(!clazz.isAnnotationPresent(Adaptive.class)) {
            if(StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if(!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                        name + " not existed(Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
            cachedInstances.remove(name);
        }
        else {
            if(cachedAdaptiveClass == null) {
                throw new IllegalStateException("Adaptive Extension not existed(Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
            cachedAdaptiveInstance.set(null);
        }
    }

    /**
     * 获取一个SPI扩展接口的有@Adaptive扩展实现类实例(实现类有@Adatptive注解标记的)
     * 整体逻辑：
     * （1）从缓存中获取，Holder<Object> cachedAdaptiveInstance：持有@Adaptive标记的扩展实现类实例
     * （2）若缓存未命中，则创建一个新的实例，并置入缓存cachedAdaptiveInstance
     * @return
     */
    @SuppressWarnings("unchecked")
    public T getAdaptiveExtension() {
        //Holder<Object> cachedAdaptiveInstance：持有@Adaptive标记的扩展实现类实例
        Object instance = cachedAdaptiveInstance.get();
        if (instance == null) {
            if(createAdaptiveInstanceError == null) {
                synchronized (cachedAdaptiveInstance) {
                    instance = cachedAdaptiveInstance.get();
                    if (instance == null) {
                        try {
                            //缓存不存在,就创建@Adaptive标记的扩展实现类实例
                            instance = createAdaptiveExtension();
                            cachedAdaptiveInstance.set(instance);
                        } catch (Throwable t) {
                            createAdaptiveInstanceError = t;
                            throw new IllegalStateException("fail to create adaptive instance: " + t.toString(), t);
                        }
                    }
                }
            }
            else {
                throw new IllegalStateException("fail to create adaptive instance: " + createAdaptiveInstanceError.toString(), createAdaptiveInstanceError);
            }
        }

        return (T) instance;
    }

    private IllegalStateException findException(String name) {
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (entry.getKey().toLowerCase().contains(name.toLowerCase())) {
                return entry.getValue();
            }
        }
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);


        int i = 1;
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if(i == 1) {
                buf.append(", possible causes: ");
            }

            buf.append("\r\n(");
            buf.append(i ++);
            buf.append(") ");
            buf.append(entry.getKey());
            buf.append(":\r\n");
            buf.append(StringUtils.toString(entry.getValue()));
        }
        return new IllegalStateException(buf.toString());
    }

    /**
     * 整体逻辑：
     * （1）通过扩展名name找到扩展实现类Class，可能会触发SPI文件加载解析getExtensionClasses()
     * （2）根据Class通过反射创建扩展实现类实例，并完成依赖注入
     * （3）如果扩展接口有扩展包装类，实例化包装类，并将当前扩展实现类实例instance，通过构造函数注入到每个包装类
     * （4）返回实例
     *          如果有包装类，那么是所有包装类中最后一个包装类的实例
     *          如果没有包装咧，那么是instance是扩展实现类的实例
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    private T createExtension(String name) {
        //根据name获取扩张实现类的Class，为空则抛异常，getExtensionClasses会触发SPI文件加载
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null) {
            throw findException(name);
        }
        try {
            //缓存中获取扩展实现类实例，获取不到则通过反射创建实例并置入缓存
            T instance = (T) EXTENSION_INSTANCES.get(clazz);
            if (instance == null) {
                EXTENSION_INSTANCES.putIfAbsent(clazz, (T) clazz.newInstance());
                instance = (T) EXTENSION_INSTANCES.get(clazz);
            }
            //2. 接口扩展实现类通过setter依赖注入，依赖组件先从SPI机制构造查找，再从spring容器查找
            injectExtension(instance);

            //如果接口的扩展实现类有包装类(有接口类型的构造函数)
            //遍历所有的包装类，将当前扩展实现类实例instance，通过构造函数注入到每个包装类
            //最后返回的instance是遍历到的最后一个包装类的实例，这也是dubbo的aop实现机制
            Set<Class<?>> wrapperClasses = cachedWrapperClasses;
            if (wrapperClasses != null && wrapperClasses.size() > 0) {
                for (Class<?> wrapperClass : wrapperClasses) {
                    //遍历wrapper类，包装实例并为wrapper类 injectExtension 注入扩展实现
                    instance = injectExtension((T) wrapperClass.getConstructor(type).newInstance(instance));
                }
            }

            //如果没有包装类，那么是instance是扩展实现类的实例，否则是包装类的实例
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance(name: " + name + ", class: " +
                    type + ")  could not be instantiated: " + t.getMessage(), t);
        }
    }

    /**
     * 通过setter给扩展实现类实例依赖注入属性
     * 根据setter方法对应的参数类型和property名称从ExtensionFactory中查询，如果有获取到实例，那么就进行注入操作
     *
     * setter方法字段类型可以是SPI接口类型，或者是Spring bean类型
     * 依赖注入的字段，是通过ExtensionLoader的objectFactory获取到的
     * objectFacotry 会根据先后通过spi机制和从spring 容器里获取属性对象并注入。
     * @param instance
     * @return
     */
    private T injectExtension(T instance) {
        try {
            if (objectFactory != null) {
                //遍历扩展实现类实例的所有的方法
                for (Method method : instance.getClass().getMethods()) {
                    //获取所有的public，并且只有一个参数，set方法
                    if (method.getName().startsWith("set")
                            && method.getParameterTypes().length == 1
                            && Modifier.isPublic(method.getModifiers())) {
                        //获取参数类型
                        Class<?> pt = method.getParameterTypes()[0];
                        try {
                            //objectFactory.getExtension工厂获取注入类型实例，根据set方法参数类型及set方法截取set之后剩余部分取小写
                            //根据set方法名构造要赋值的属性名，setProtocol方法property=protocol
                            String property = method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
                            //根据类型，名称信息从ExtensionFactory获取
                            //通过getExtension的方法获取属性对象，所以还要看getExtension的实现
                            Object object = objectFactory.getExtension(pt, property);
                            if (object != null) {
                                ///如果不为空，set方法的参数是扩展点类型，那么进行注入
                                //通过反射，赋值对象属性
                                method.invoke(instance, object);
                            }
                        } catch (Exception e) {
                            logger.error("fail to inject via method " + method.getName()
                                    + " of interface " + type.getName() + ": " + e.getMessage(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return instance;
    }
    
	private Class<?> getExtensionClass(String name) {
	    if (type == null)
	        throw new IllegalArgumentException("Extension type == null");
	    if (name == null)
	        throw new IllegalArgumentException("Extension name == null");
	    Class<?> clazz = getExtensionClasses().get(name);
	    if (clazz == null)
	        throw new IllegalStateException("No such extension \"" + name + "\" for " + type.getName() + "!");
	    return clazz;
	}

    /**
     * SPI文件加载解析，获取扩展接口对应的所有的扩展实现类Class，并置入缓存Map
     * key：扩展配置文件中配置的key，val：扩展配置文件中配置的val，即扩展实现类全类名
     * (1) 先从缓存获取Map<String, Class<?>>
     * (2) 缓存获取不到加载解析SPI配置文件，然后置入缓存
     * @return
     */
	private Map<String, Class<?>> getExtensionClasses() {
        //classes：key 扩展配置文件中配置的key，val 扩展配置文件中配置的val
        Map<String, Class<?>> classes = cachedClasses.get();
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                //不存在则加载并缓存
                if (classes == null) {
                    //加载类路径中的SPI配置文件，构造cachedClasses
                    classes = loadExtensionClasses();
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
	}

    // 此方法已经getExtensionClasses方法同步过。
    /**
     * 加载类路径中的SPI配置文件，构造cachedClasses，以及赋值相应属性
     * 1. cachedDefaultName：设置默认的扩展实现类名称
     * 2. 从3个目录加载：META-INF/dubbo/internal/、META-INF/dubbo/、META-INF/services/
     * @return
     */
    private Map<String, Class<?>> loadExtensionClasses() {
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if(defaultAnnotation != null) {//获取SPI注解，SPI(value="xxx")，xxx代表扩展接口的默认扩展实现类是谁
            String value = defaultAnnotation.value();
            if(value != null && (value = value.trim()).length() > 0) {
                String[] names = NAME_SEPARATOR.split(value);//逗号分隔value
                if(names.length > 1) {//只能有一个默认扩展实现类
                    throw new IllegalStateException("more than 1 default extension name on extension " + type.getName()
                            + ": " + Arrays.toString(names));
                }
                if(names.length == 1) cachedDefaultName = names[0];
            }
        }

        //loadFile方法加载扩展类：3个目录，META-INF/dubbo/internal/、META-INF/dubbo/、META-INF/services/
        //key: 配置文件中的key， val:扩展实现类Class
        Map<String, Class<?>> extensionClasses = new HashMap<String, Class<?>>();
        loadFile(extensionClasses, DUBBO_INTERNAL_DIRECTORY);
        loadFile(extensionClasses, DUBBO_DIRECTORY);
        loadFile(extensionClasses, SERVICES_DIRECTORY);
        return extensionClasses;
    }

    /**
     * loadFile方法加载扩展类：3个目录，META-INF/dubbo/internal/、META-INF/dubbo/、META-INF/services/
     * Map<String, Class<?>> extensionClasses
     *          key: 配置文件中的key， val:扩展实现类Class
     * 1. 如果扩展实现类有@Adaptive，则将Class赋值给成员变量cachedAdaptiveClass
     * 2. 如果扩展实现类是扩展接口type的包装类，所有包装类Class都会存入Set，Set<Class<?>> cachedWrapperClasses
     * 3. 扩展实现类没有@Adaptive和也不是wapper类，则置入extensionClasses，
     * @param extensionClasses
     * @param dir
     */
    private void loadFile(Map<String, Class<?>> extensionClasses, String dir) {
        //拼接扩展接口全类名，作为文件名，eg：META-INF/dubbo/internal/com.alibaba.dubbo.rpc.Protocol
        String fileName = dir + type.getName();
        try {
            Enumeration<java.net.URL> urls;
            ClassLoader classLoader = findClassLoader();
            //通过类加载器，获取类路径下指定文件名的文件URL
            if (classLoader != null) {
                urls = classLoader.getResources(fileName);
            } else {
                urls = ClassLoader.getSystemResources(fileName);
            }
            if (urls != null) {
                while (urls.hasMoreElements()) { //遍历文件URL
                    java.net.URL url = urls.nextElement();
                    try {
                        //IO流读取文件
                        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), "utf-8"));
                        try {
                            String line = null;
                            while ((line = reader.readLine()) != null) { //读取文件的每一行
                                final int ci = line.indexOf('#');//#为注释，故#后的内容忽略
                                if (ci >= 0) line = line.substring(0, ci);
                                line = line.trim();
                                if (line.length() > 0) {
                                    try {
                                        String name = null;
                                        int i = line.indexOf('=');
                                        if (i > 0) {
                                            //myprotocol=com.kl.dubbotest.spi.MyProtocol
                                            name = line.substring(0, i).trim();//扩展名
                                            line = line.substring(i + 1).trim();//扩展全类名
                                        }
                                        if (line.length() > 0) {
                                            //通过反射，获取实现类Class
                                            Class<?> clazz = Class.forName(line, true, classLoader);
                                            if (! type.isAssignableFrom(clazz)) { //扩展实现类是否实现了扩展接口type
                                                throw new IllegalStateException("Error when load extension class(interface: " +
                                                        type + ", class line: " + clazz.getName() + "), class " 
                                                        + clazz.getName() + "is not subtype of interface.");
                                            }
                                            //扩展实现类是否有@Adaptive注解，置入缓存cachedAdaptiveClass
                                            if (clazz.isAnnotationPresent(Adaptive.class)) {
                                                if(cachedAdaptiveClass == null) {
                                                    cachedAdaptiveClass = clazz;
                                                } else if (! cachedAdaptiveClass.equals(clazz)) {
                                                    //一个扩展接口的SPI实现，只能有一个扩展实现类是@Adaptive标记的
                                                    throw new IllegalStateException("More than 1 adaptive class found: "
                                                            + cachedAdaptiveClass.getClass().getName()
                                                            + ", " + clazz.getClass().getName());
                                                }
                                            } else {
                                                //扩展实现类不存在@Adaptive
                                                try {
                                                    //扩展实现类是否有参数为扩展接口类型的（比如 com.alibaba.dubbo.rpc.Protocol类型，并且1个参数）的构造函数
                                                    //表示它是个接口包装类Wrapper
                                                    //getConstructor若不存在这样的构造函数会报错
                                                    clazz.getConstructor(type);
                                                    Set<Class<?>> wrappers = cachedWrapperClasses;
                                                    if (wrappers == null) {
                                                        cachedWrapperClasses = new ConcurrentHashSet<Class<?>>();
                                                        wrappers = cachedWrapperClasses;
                                                    }
                                                    //将扩展实现类包装类Class缓存至cachedWrapperClasses
                                                    wrappers.add(clazz);
                                                } catch (NoSuchMethodException e) { //不存在参数为Class type的构造函数，即不是包装Wrapper类型
                                                    clazz.getConstructor();//校验是否存在无参构造函数
                                                    //不在在name，只有val，即SPI配置文件中只有val没有key
                                                    if (name == null || name.length() == 0) {
                                                        //name为（eg: AdaptiveExtensionFactory去除ExtensionFactory，name为:adaptive）
                                                        name = findAnnotationName(clazz);
                                                        if (name == null || name.length() == 0) {
                                                            if (clazz.getSimpleName().length() > type.getSimpleName().length()
                                                                    && clazz.getSimpleName().endsWith(type.getSimpleName())) {
                                                                name = clazz.getSimpleName().substring(0, clazz.getSimpleName().length() - type.getSimpleName().length()).toLowerCase();
                                                            } else {
                                                                throw new IllegalStateException("No such extension name for the class " + clazz.getName() + " in the config " + url);
                                                            }
                                                        }
                                                    }
                                                    //逗号分隔name属性
                                                    String[] names = NAME_SEPARATOR.split(name);
                                                    if (names != null && names.length > 0) {
                                                        //扩展实现类，是否有Activate注解
                                                        Activate activate = clazz.getAnnotation(Activate.class);
                                                        if (activate != null) {
                                                            //如果有，将@Activate注解以names[0]为key缓存至cachedActivates
                                                            cachedActivates.put(names[0], activate);
                                                        }
                                                        //遍历names，将name以clazz为key缓存至cachedNames
                                                        for (String n : names) {
                                                            if (! cachedNames.containsKey(clazz)) {
                                                                //map  key：扩展实现类Class, val：配置文件中的key
                                                                cachedNames.put(clazz, n);
                                                            }

                                                            Class<?> c = extensionClasses.get(n);
                                                            if (c == null) {
                                                                //将扩展实现类Class以name为key放入extensionClasses
                                                                //map  key: 配置文件中的key， val:扩展实现类Class
                                                                //@Adaptive扩展实现类和wapper类都不在extensionClasses里，联系上下文
                                                                extensionClasses.put(n, clazz);
                                                            } else if (c != clazz) {
                                                                throw new IllegalStateException("Duplicate extension " + type.getName() + " name " + n + " on " + c.getName() + " and " + clazz.getName());
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } catch (Throwable t) {
                                        IllegalStateException e = new IllegalStateException("Failed to load extension class(interface: " + type + ", class line: " + line + ") in " + url + ", cause: " + t.getMessage(), t);
                                        exceptions.put(line, e);
                                    }
                                }
                            } // end of while read lines
                        } finally {
                            reader.close();
                        }
                    } catch (Throwable t) {
                        logger.error("Exception when load extension class(interface: " +
                                            type + ", class file: " + url + ") in " + url, t);
                    }
                } // end of while urls
            }
        } catch (Throwable t) {
            logger.error("Exception when load extension class(interface: " +
                    type + ", description file: " + fileName + ").", t);
        }
    }
    
    @SuppressWarnings("deprecation")
    private String findAnnotationName(Class<?> clazz) {
        com.alibaba.dubbo.common.Extension extension = clazz.getAnnotation(com.alibaba.dubbo.common.Extension.class);
        if (extension == null) {
            String name = clazz.getSimpleName();
            if (name.endsWith(type.getSimpleName())) {
                name = name.substring(0, name.length() - type.getSimpleName().length());
            }
            return name.toLowerCase();
        }
        return extension.value();
    }

    /**
     * 创建@Adaptive标记的扩展实现类实例
     * @return
     */
    @SuppressWarnings("unchecked")
    private T createAdaptiveExtension() {
        try {
            //获取@Adaptive标记的扩展实现类Class，通过反射创建实例，同时要走依赖注入流程
            //AdaptiveExtensionClass已在SPI文件解析时赋值
            return injectExtension((T) getAdaptiveExtensionClass().newInstance());
        } catch (Exception e) {
            throw new IllegalStateException("Can not create adaptive extenstion " + type + ", cause: " + e.getMessage(), e);
        }
    }

    /**
     * 获取@Adaptive标记的扩展实现类Class
     * @return
     */
    private Class<?> getAdaptiveExtensionClass() {
        //如果有必要，触发SPI文件加载流程
        //找到@Adaptive标记的扩展实现类Class，赋值给cachedAdaptiveClass
        getExtensionClasses();
        if (cachedAdaptiveClass != null) {
            return cachedAdaptiveClass;
        }

        //不存在有@Adaptive注解的扩展实现类，那肯定是在待扩展接口方法上
        //使用dubbo动态生成生成java类字符串，动态编译生成想要的class
        return cachedAdaptiveClass = createAdaptiveExtensionClass();
    }
    
    private Class<?> createAdaptiveExtensionClass() {
        //生成@Adaptive类源码字符串
        String code = createAdaptiveExtensionClassCode();
        ClassLoader classLoader = findClassLoader();
        //通过SPI获取java编译器
        com.alibaba.dubbo.common.compiler.Compiler compiler = ExtensionLoader.getExtensionLoader(com.alibaba.dubbo.common.compiler.Compiler.class).getAdaptiveExtension();
        //编译源码返回Class
        return compiler.compile(code, classLoader);
    }
    
    private String createAdaptiveExtensionClassCode() {
        StringBuilder codeBuidler = new StringBuilder();
        Method[] methods = type.getMethods();
        boolean hasAdaptiveAnnotation = false;
        for(Method m : methods) {
            if(m.isAnnotationPresent(Adaptive.class)) {
                hasAdaptiveAnnotation = true;
                break;
            }
        }
        // 完全没有@Adaptive方法，则不需要生成Adaptive类，抛出异常
        if(! hasAdaptiveAnnotation)
            throw new IllegalStateException("No adaptive method on extension " + type.getName() + ", refuse to create the adaptive class!");
        
        codeBuidler.append("package " + type.getPackage().getName() + ";");
        codeBuidler.append("\nimport " + ExtensionLoader.class.getName() + ";");
        codeBuidler.append("\npublic class " + type.getSimpleName() + "$Adpative" + " implements " + type.getCanonicalName() + " {");
        
        for (Method method : methods) {
            Class<?> rt = method.getReturnType();
            Class<?>[] pts = method.getParameterTypes();
            Class<?>[] ets = method.getExceptionTypes();

            Adaptive adaptiveAnnotation = method.getAnnotation(Adaptive.class);
            StringBuilder code = new StringBuilder(512);
            if (adaptiveAnnotation == null) {
                code.append("throw new UnsupportedOperationException(\"method ")
                        .append(method.toString()).append(" of interface ")
                        .append(type.getName()).append(" is not adaptive method!\");");
            } else {
                int urlTypeIndex = -1;
                for (int i = 0; i < pts.length; ++i) {
                    if (pts[i].equals(URL.class)) {
                        urlTypeIndex = i;
                        break;
                    }
                }
                // 有类型为URL的参数
                if (urlTypeIndex != -1) {
                    // Null Point check
                    String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"url == null\");",
                                    urlTypeIndex);
                    code.append(s);
                    
                    s = String.format("\n%s url = arg%d;", URL.class.getName(), urlTypeIndex); 
                    code.append(s);
                }
                // 参数没有URL类型
                else {
                    String attribMethod = null;
                    
                    // 找到参数的URL属性
                    LBL_PTS:
                    for (int i = 0; i < pts.length; ++i) {
                        Method[] ms = pts[i].getMethods();
                        for (Method m : ms) {
                            String name = m.getName();
                            if ((name.startsWith("get") || name.length() > 3)
                                    && Modifier.isPublic(m.getModifiers())
                                    && !Modifier.isStatic(m.getModifiers())
                                    && m.getParameterTypes().length == 0
                                    && m.getReturnType() == URL.class) {
                                urlTypeIndex = i;
                                attribMethod = name;
                                break LBL_PTS;
                            }
                        }
                    }
                    if(attribMethod == null) {
                        throw new IllegalStateException("fail to create adative class for interface " + type.getName()
                        		+ ": not found url parameter or url attribute in parameters of method " + method.getName());
                    }
                    
                    // Null point check
                    String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"%s argument == null\");",
                                    urlTypeIndex, pts[urlTypeIndex].getName());
                    code.append(s);
                    s = String.format("\nif (arg%d.%s() == null) throw new IllegalArgumentException(\"%s argument %s() == null\");",
                                    urlTypeIndex, attribMethod, pts[urlTypeIndex].getName(), attribMethod);
                    code.append(s);

                    s = String.format("%s url = arg%d.%s();",URL.class.getName(), urlTypeIndex, attribMethod); 
                    code.append(s);
                }
                
                String[] value = adaptiveAnnotation.value();
                // 没有设置Key，则使用“扩展点接口名的点分隔 作为Key
                if(value.length == 0) {
                    char[] charArray = type.getSimpleName().toCharArray();
                    StringBuilder sb = new StringBuilder(128);
                    for (int i = 0; i < charArray.length; i++) {
                        if(Character.isUpperCase(charArray[i])) {
                            if(i != 0) {
                                sb.append(".");
                            }
                            sb.append(Character.toLowerCase(charArray[i]));
                        }
                        else {
                            sb.append(charArray[i]);
                        }
                    }
                    value = new String[] {sb.toString()};
                }
                
                boolean hasInvocation = false;
                for (int i = 0; i < pts.length; ++i) {
                    if (pts[i].getName().equals("com.alibaba.dubbo.rpc.Invocation")) {
                        // Null Point check
                        String s = String.format("\nif (arg%d == null) throw new IllegalArgumentException(\"invocation == null\");", i);
                        code.append(s);
                        s = String.format("\nString methodName = arg%d.getMethodName();", i); 
                        code.append(s);
                        hasInvocation = true;
                        break;
                    }
                }
                
                String defaultExtName = cachedDefaultName;
                String getNameCode = null;
                for (int i = value.length - 1; i >= 0; --i) {
                    if(i == value.length - 1) {
                        if(null != defaultExtName) {
                            if(!"protocol".equals(value[i]))
                                if (hasInvocation) 
                                    getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                                else
                                    getNameCode = String.format("url.getParameter(\"%s\", \"%s\")", value[i], defaultExtName);
                            else
                                getNameCode = String.format("( url.getProtocol() == null ? \"%s\" : url.getProtocol() )", defaultExtName);
                        }
                        else {
                            if(!"protocol".equals(value[i]))
                                if (hasInvocation) 
                                    getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                                else
                                    getNameCode = String.format("url.getParameter(\"%s\")", value[i]);
                            else
                                getNameCode = "url.getProtocol()";
                        }
                    }
                    else {
                        if(!"protocol".equals(value[i]))
                            if (hasInvocation) 
                                getNameCode = String.format("url.getMethodParameter(methodName, \"%s\", \"%s\")", value[i], defaultExtName);
                            else
                                getNameCode = String.format("url.getParameter(\"%s\", %s)", value[i], getNameCode);
                        else
                            getNameCode = String.format("url.getProtocol() == null ? (%s) : url.getProtocol()", getNameCode);
                    }
                }
                code.append("\nString extName = ").append(getNameCode).append(";");
                // check extName == null?
                String s = String.format("\nif(extName == null) " +
                		"throw new IllegalStateException(\"Fail to get extension(%s) name from url(\" + url.toString() + \") use keys(%s)\");",
                        type.getName(), Arrays.toString(value));
                code.append(s);
                
                s = String.format("\n%s extension = (%<s)%s.getExtensionLoader(%s.class).getExtension(extName);",
                        type.getName(), ExtensionLoader.class.getSimpleName(), type.getName());
                code.append(s);
                
                // return statement
                if (!rt.equals(void.class)) {
                    code.append("\nreturn ");
                }

                s = String.format("extension.%s(", method.getName());
                code.append(s);
                for (int i = 0; i < pts.length; i++) {
                    if (i != 0)
                        code.append(", ");
                    code.append("arg").append(i);
                }
                code.append(");");
            }
            
            codeBuidler.append("\npublic " + rt.getCanonicalName() + " " + method.getName() + "(");
            for (int i = 0; i < pts.length; i ++) {
                if (i > 0) {
                    codeBuidler.append(", ");
                }
                codeBuidler.append(pts[i].getCanonicalName());
                codeBuidler.append(" ");
                codeBuidler.append("arg" + i);
            }
            codeBuidler.append(")");
            if (ets.length > 0) {
                codeBuidler.append(" throws ");
                for (int i = 0; i < ets.length; i ++) {
                    if (i > 0) {
                        codeBuidler.append(", ");
                    }
                    codeBuidler.append(pts[i].getCanonicalName());
                }
            }
            codeBuidler.append(" {");
            codeBuidler.append(code.toString());
            codeBuidler.append("\n}");
        }
        codeBuidler.append("\n}");
        if (logger.isDebugEnabled()) {
            logger.debug(codeBuidler.toString());
        }
        return codeBuidler.toString();
    }

    private static ClassLoader findClassLoader() {
        return  ExtensionLoader.class.getClassLoader();
    }
    
    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }
    
}