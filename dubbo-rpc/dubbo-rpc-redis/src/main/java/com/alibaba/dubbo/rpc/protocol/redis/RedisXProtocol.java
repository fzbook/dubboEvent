package com.alibaba.dubbo.rpc.protocol.redis;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.rpc.Exporter;
import com.alibaba.dubbo.rpc.Invocation;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Result;
import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.dubbo.rpc.RpcResult;
import com.alibaba.dubbo.rpc.protocol.AbstractInvoker;
import com.alibaba.dubbo.rpc.protocol.AbstractProtocol;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * 实现功能更加强大的 Redis 客户端访问机制
 * 
 * @author fangzhou
 *
 */
public class RedisXProtocol extends AbstractProtocol {

	public static final int DEFAULT_PORT = 6379;
	private Log logger = LogFactory.getLog(RedisXProtocol.class);
	private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<String, Notifier>();
	private final Map<String, Method> methodsMap = new ConcurrentHashMap<String, Method>();
	private final Map<String, JedisPool> jedisPools = new ConcurrentHashMap<String, JedisPool>();

	@Override
	public int getDefaultPort() {
		// TODO Auto-generated method stub
		return DEFAULT_PORT;
	}

	@Override
	public <T> Exporter<T> export(Invoker<T> invoker) throws RpcException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unsupported export redis service. url: " + invoker.getUrl());
	}

	@Override
	public void destroy() {
		super.destroy();
		try {
			for (Notifier notifier : notifiers.values()) {
				notifier.shutdown();
			}
		} catch (Throwable t) {
			logger.warn(t.getMessage(), t);
		}
	}

	@Override
	public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
		// TODO Auto-generated method stub
		GenericObjectPool.Config config = new GenericObjectPool.Config();
		config.testOnBorrow = url.getParameter("test.on.borrow", true);
		config.testOnReturn = url.getParameter("test.on.return", false);
		config.testWhileIdle = url.getParameter("test.while.idle", false);
		if (url.getParameter("max.idle", 0) > 0)
			config.maxIdle = url.getParameter("max.idle", 0);
		if (url.getParameter("min.idle", 0) > 0)
			config.minIdle = url.getParameter("min.idle", 0);
		if (url.getParameter("max.active", 0) > 0)
			config.maxActive = url.getParameter("max.active", 0);
		if (url.getParameter("max.wait", url.getParameter("timeout", 0)) > 0)
			config.maxWait = url.getParameter("max.wait", url.getParameter("timeout", 0));
		if (url.getParameter("num.tests.per.eviction.run", 0) > 0)
			config.numTestsPerEvictionRun = url.getParameter("num.tests.per.eviction.run", 0);
		if (url.getParameter("time.between.eviction.runs.millis", 0) > 0)
			config.timeBetweenEvictionRunsMillis = url.getParameter("time.between.eviction.runs.millis", 0);
		if (url.getParameter("min.evictable.idle.time.millis", 0) > 0)
			config.minEvictableIdleTimeMillis = url.getParameter("min.evictable.idle.time.millis", 0);

		String cluster = url.getParameter("cluster", "failover");
		if (!"failover".equals(cluster) && !"replicate".equals(cluster)) {
			throw new IllegalArgumentException("Unsupported redis cluster: " + cluster
					+ ". The redis cluster only supported failover or replicate.");
		}

		List<String> addresses = new ArrayList<String>();
		addresses.add(url.getAddress());
		String[] backups = url.getParameter(Constants.BACKUP_KEY, new String[0]);
		if (backups != null && backups.length > 0) {
			addresses.addAll(Arrays.asList(backups));
		}
		for (String address : addresses) {
			int i = address.indexOf(':');
			String host;
			int port;
			if (i > 0) {
				host = address.substring(0, i);
				port = Integer.parseInt(address.substring(i + 1));
			} else {
				host = address;
				port = DEFAULT_PORT;
			}
			this.jedisPools.put(address, new JedisPool(config, host, port,
					url.getParameter(Constants.TIMEOUT_KEY, Constants.DEFAULT_TIMEOUT)));
		}

		String service = url.getServiceInterface();
		Notifier pubsub = notifiers.get(service);
		if (pubsub == null) {
			pubsub = new Notifier(url);
			notifiers.putIfAbsent(service, pubsub);
			pubsub.start();
		}

		Method[] typeAllMethods = type.getMethods();
		Method[] redisAllMethods = Jedis.class.getMethods();
		for (Method typeMethod : typeAllMethods) {
			if (typeMethod.getDeclaringClass() == Object.class)
				continue;
			boolean find = false;
			for (Method redisMethod : redisAllMethods) {
				if (redisMethod.getDeclaringClass() == Object.class)
					continue;
				if (!typeMethod.getReturnType().getName().equals(redisMethod.getReturnType().getName()))
					continue;
				if (!typeMethod.getName().equals(redisMethod.getName()))
					continue;
				String typeParam = Arrays.toString(typeMethod.getParameterTypes());
				String redisParam = Arrays.toString(redisMethod.getParameterTypes());
				if (!typeParam.equals(redisParam))
					continue;
				find = true;
				String key = redisMethod.getName() + "_" + redisParam;
				methodsMap.put(key, redisMethod);
				break;
			}
			if (!find)
				throw new UnsupportedOperationException("Unsupported export redis service. url: " + url);
		}

		final URL invokerUrl = url;
		final Class<T> invokerType = type;

		return new AbstractInvoker<T>(invokerType, invokerUrl) {

			@Override
			protected Result doInvoke(Invocation invocation) throws Throwable {
				String key = invocation.getMethodName() + "_" + Arrays.toString(invocation.getParameterTypes());
				Method m = methodsMap.get(key);
				if (m == null)
					throw new UnsupportedOperationException("Unsupported export redis service. url: " + url);
				Class<?> rt = m.getReturnType();
				boolean completed = false;
				Object result = null;
				for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
					if (completed)
						break;
					JedisPool jedisPool = entry.getValue();
					Jedis jedis = null;
					boolean isBroken = false;
					try {
						jedis = jedisPool.getResource();
						try {
							result = m.invoke(jedis, invocation.getArguments());
							completed = true;
							break;
						} catch (JedisConnectionException e) {
							isBroken = true;
						} catch (Throwable t) {
							completed = true;
							RpcException re = new RpcException("Failed to invoke memecached service method. interface: "
									+ type.getName() + ", method: " + invocation.getMethodName() + ", url: " + url
									+ ", cause: " + t.getMessage(), t);
							if (t instanceof TimeoutException || t instanceof SocketTimeoutException) {
								re.setCode(RpcException.TIMEOUT_EXCEPTION);
							} else if (t instanceof JedisConnectionException || t instanceof IOException) {
								re.setCode(RpcException.NETWORK_EXCEPTION);
							} else if (t instanceof JedisDataException) {
								re.setCode(RpcException.SERIALIZATION_EXCEPTION);
							}
							throw re;
						} finally {
							if (isBroken) {
								jedisPool.returnBrokenResource(jedis);
							} else {
								jedisPool.returnResource(jedis);
							}
							jedis = null;
						}
					} catch (Throwable t) {
						//
					}
				}
				if (!Void.TYPE.equals(rt) && result != null)
					return new RpcResult(result);
				else
					return new RpcResult();
			}

		};
	}

	private class Notifier extends Thread {

		private volatile boolean running = false;

		private RedisXPubSub jedisPubSub = null;

		private RedisXSubscriberListener subscriber = null;

		private final int reconnectPeriod;

		private String[] watchTargets = null;

        private volatile Jedis jedis;

		public Notifier(URL url) {
			super.setDaemon(true);
			super.setName("redisSubscribe");
			Map<String, String> listeners = url.getParameters();
			ArrayList<String> listenerList = new ArrayList<String>();
			listenerList.clear();
			boolean usingDubbo = false;
			String listenerName = "true";
			for (Map.Entry<String, String> param : listeners.entrySet()) {
				String key = param.getKey();
				String value = param.getValue();
				if (key.equals("subscribe.class")) {
					listenerName = value;
				} else if (key.equals("subscribe.name")) {
					usingDubbo = true;
					listenerName = value;
				} else if (key.startsWith("subscribe.watch.")) {
					listenerList.add(value);
				}
			}

			if (!listenerList.isEmpty()) {
				watchTargets = new String[listenerList.size()];
				int i = 0;
				for (String str : listenerList) {
					watchTargets[i] = str;
				}
				logger.info("Subscribe keys:" + Arrays.toString(watchTargets));
			}

			if (usingDubbo) {
				subscriber = ExtensionLoader.getExtensionLoader(RedisXSubscriberListener.class).getExtension(listenerName);
			} else {
				try {
					subscriber = (RedisXSubscriberListener) Class.forName(listenerName).newInstance();
				} catch (Throwable t) {
					//
				}
			}

			this.jedisPubSub = new RedisXPubSub(subscriber);
			this.reconnectPeriod = url.getParameter(Constants.REGISTRY_RECONNECT_PERIOD_KEY,
					Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD);
		}

		@Override
		public synchronized void start() {
			// TODO Auto-generated method stub
			if (watchTargets == null || watchTargets.length == 0)
				return;
			running = true;
			super.start();
		}

		@Override
		public void run() {
			while (running) {
				for (Map.Entry<String, JedisPool> entry : jedisPools.entrySet()) {
					JedisPool jedisPool = entry.getValue();
					try {
						if (watchTargets == null || watchTargets.length == 0) {
							sleep(reconnectPeriod);
							continue;
						}
						jedis = jedisPool.getResource();
						boolean isBroken = false;
						try {
							jedisPubSub.setJedisPool(jedisPool);
							while (running) {
								jedis.subscribe(jedisPubSub, watchTargets);
							}
						} catch (JedisConnectionException e) {
							isBroken = true;
						} finally {
							if (isBroken) {
								jedisPool.returnBrokenResource(jedis);
							} else {
								jedisPool.returnResource(jedis);
							}
						}
					} catch (Throwable t) {
						logger.warn("Failed to subscribe from redis, cause: " + t.getMessage(), t);
						try {
							sleep(reconnectPeriod);
						} catch (Throwable t0) {
							//
						}
					}
				}
			}
		}

		public void shutdown() {
			try {
				if (running) {
					running = false;
					jedis.disconnect();
				}
			} catch (Throwable t) {
				logger.warn(t.getMessage(), t);
			}
		}

	}
}
