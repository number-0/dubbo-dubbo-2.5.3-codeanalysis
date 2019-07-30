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
package com.alibaba.dubbo.rpc.proxy.javassist;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.bytecode.Proxy;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.proxy.AbstractProxyFactory;
import com.alibaba.dubbo.rpc.proxy.AbstractProxyInvoker;
import com.alibaba.dubbo.rpc.proxy.InvokerInvocationHandler;

/**
 * JavaassistRpcProxyFactory 

 * @author william.liangf
 */
public class JavassistProxyFactory extends AbstractProxyFactory {

    @SuppressWarnings("unchecked")
    public <T> T getProxy(Invoker<T> invoker, Class<?>[] interfaces) {
        // 生成 Proxy 子类（Proxy 是抽象类）。并调用 Proxy 子类的 newInstance 方法创建 Proxy 实例
        return (T) Proxy.getProxy(interfaces).newInstance(new InvokerInvocationHandler(invoker));
    }

    /**
     * 整体流程：
     * （1）通过服务实现类动态生成对应的代理类wrapper
     * （2）new一个抽象的Invoker(AbstractProxyInvoker)，实现抽象方法doInvoke，doInvoke内部调用动态生成的代理类的invokeMethod方法
     * @param proxy 服务实现类
     * @param type 服务接口
     * @param url
     * @param <T>
     * @return
     */
    public <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) {
        //Wrapper类通过服务实现类生成动态对应的代理类Class
        // TODO Wrapper类不能正确处理带$的类名
        final Wrapper wrapper = Wrapper.getWrapper(proxy.getClass().getName().indexOf('$') < 0 ? proxy.getClass() : type);
        //实现抽象类AbstractProxyInvoker抽象方法doInvoke，并调用(proxy, type, url)构造函数实例化匿名类
        return new AbstractProxyInvoker<T>(proxy, type, url) {
            @Override
            protected Object doInvoke(T proxy, String methodName, 
                                      Class<?>[] parameterTypes, 
                                      Object[] arguments) throws Throwable {
                //调用代理类的invokeMethod方法
                return wrapper.invokeMethod(proxy, methodName, parameterTypes, arguments);
            }
        };
    }


    //*****  如下为动态生成的wrapper
//    package com.kl.dubbotest.provider.export;
//
//import com.alibaba.dubbo.common.bytecode.ClassGenerator;
//import com.alibaba.dubbo.common.bytecode.NoSuchPropertyException;
//import com.alibaba.dubbo.common.bytecode.Wrapper;
//import java.util.Map;
//
//    public class Wrapper0 extends Wrapper implements ClassGenerator.DC {
//
//        public static String[] pns;
//        public static Map pts;
//        public static String[] mns;
//        public static String[] dmns;
//        public static Class[] mts0;
//
//        public String[] getPropertyNames() {
//            return pns;
//        }
//
//        public boolean hasProperty(String paramString) {
//            return pts.containsKey(paramString);
//        }
//
//        public Class getPropertyType(String paramString) {
//            return (Class) pts.get(paramString);
//        }
//
//        public String[] getMethodNames() {
//            return mns;
//        }
//
//        public String[] getDeclaredMethodNames() {
//            return dmns;
//        }
//
//        public void setPropertyValue(Object paramObject1, String paramString, Object paramObject2) {
//            try {
//                com.kl.dubbotest.provider.export.ProviderExport localProviderExport = (com.kl.dubbotest.provider.export.ProviderExport) paramObject1;
//            } catch (Throwable localThrowable) {
//                throw new IllegalArgumentException(localThrowable);
//            }
//            throw new NoSuchPropertyException("Not found property \"" + paramString
//                    + "\" filed or setter method in class com.kl.dubbotest.provider.export.ProviderExport.");
//        }
//
//        public Object getPropertyValue(Object paramObject, String paramString) {
//            try {
//                com.kl.dubbotest.provider.export.ProviderExport localProviderExport = (com.kl.dubbotest.provider.export.ProviderExport) paramObject;
//            } catch (Throwable localThrowable) {
//                throw new IllegalArgumentException(localThrowable);
//            }
//            throw new NoSuchPropertyException("Not found property \"" + paramString
//                    + "\" filed or setter method in class com.kl.dubbotest.provider.export.ProviderExport.");
//        }
//
//        /**
//         * 核心方法
//         * @param paramObject
//         * @param paramString
//         * @param paramArrayOfClass
//         * @param paramArrayOfObject
//         * @return
//         * @throws java.lang.reflect.InvocationTargetException
//         */
//        public Object invokeMethod(Object paramObject, String paramString, Class[] paramArrayOfClass,
//                Object[] paramArrayOfObject) throws java.lang.reflect.InvocationTargetException {
//            com.kl.dubbotest.provider.export.ProviderExport localProviderExport;
//            try {
//                //将接口实现类ProviderExportImpl对象赋值给localProviderExport
//                localProviderExport = ((com.kl.dubbotest.provider.export.ProviderExport) paramObject);
//            } catch (Throwable e) {
//                throw new IllegalArgumentException(e);
//            }
//            try {
//                //根据传入的方法名paramString，方法参数，执行方法
//                if ("providerExport".equals(paramString) && paramArrayOfClass.length == 1) {
//                    localProviderExport.providerExport((java.lang.String) paramArrayOfObject[0]);
//                    return "providerExport 。。。";
//                }
//            } catch (Throwable e) {
//                throw new java.lang.reflect.InvocationTargetException(e);
//            }
//            throw new com.alibaba.dubbo.common.bytecode.NoSuchMethodException("Not found method \"" + paramString
//                    + "\" in class com.kl.dubbotest.provider.export.ProviderExport.");
//        }
//
//    }




    //start --------------- 服务引用动态生成消费者的代理类 ---------------

    //--------------- Proxy0
//    package com.alibaba.dubbo.common.bytecode;
//    import com.alibaba.dubbo.common.bytecode.ClassGenerator.DC;
//    import java.lang.reflect.InvocationHandler;
//
//    public class Proxy0 extends Proxy implements DC {
//        public Object newInstance(InvocationHandler var1) {
//            return new proxy01(var1);
//        }
//
//        public Proxy0_my() {
//        }
//    }

    //--------------- Proxy01
//    package com.alibaba.dubbo.common.bytecode;
//    import com.alibaba.dubbo.rpc.service.EchoService;
//    import demo.dubbo.api.DemoService;
//    import java.lang.reflect.InvocationHandler;
//    import java.lang.reflect.Method;
//
//    public class proxy01 implements ClassGenerator.DC, EchoService, DemoService {
//        public static Method[] methods;
//        private InvocationHandler handler;
//        //实现了接口方法
//        public String sayHello(String var1) {
//            Object[] var2 = new Object[]{var1};
//            Object var3 = null;
//            try {
//                var3 = this.handler.invoke(this, methods[1], var2);
//            } catch (Throwable throwable) {
//                throwable.printStackTrace();
//            }
//            return (String)var3;
//        }
//
//        public Object $echo(Object var1) {
//            Object[] var2 = new Object[]{var1};
//            Object var3 = null;
//            try {
//                var3 = this.handler.invoke(this, methods[3], var2);
//            } catch (Throwable throwable) {
//                throwable.printStackTrace();
//            }
//            return (Object)var3;
//        }
//
//        public proxy01() {
//        }
//        //public 构造函数，这里handler是
//        //由Proxy.getProxy(interfaces).newInstance(new InvokerInvocationHandler(invoker))语句传入的InvokerInvocationHandler对象
//        public proxy01(InvocationHandler var1) {
//            this.handler = var1;
//        }
//    }

    //end --------------- 服务引用动态生成消费者的代理类 ---------------
}