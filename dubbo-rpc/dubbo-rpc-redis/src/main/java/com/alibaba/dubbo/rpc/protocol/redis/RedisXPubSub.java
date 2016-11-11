package com.alibaba.dubbo.rpc.protocol.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

class RedisXPubSub extends JedisPubSub {

	private RedisXSubscriberListener sub = null;
	private JedisPool jedisPool = null;
	private Log logger = LogFactory.getLog(JedisPubSub.class);

	public RedisXPubSub(RedisXSubscriberListener sub) {
		super();
		this.sub = sub;
	}
	
	public void setJedisPool(JedisPool jp) {
		jedisPool = jp;
	}

	@Override
	public void onMessage(String channel, String message) {
		// TODO Auto-generated method stub
		try {
			Jedis jedis = jedisPool.getResource();
			boolean isBroken = false;
			try {
				sub.onMessage(jedis, channel, message);
			} catch (JedisConnectionException e) {
				isBroken = true;
			} finally {
				if (isBroken) {
					jedisPool.returnBrokenResource(jedis);
				} else {
					jedisPool.returnResource(jedis);
				}
			}
		} catch (Throwable t) { // TODO 通知失败没有恢复机制保障
			logger.error(t.getMessage(), t);
		}
	}

	@Override
	public void onPMessage(String pattern, String channel, String message) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onSubscribe(String channel, int subscribedChannels) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onUnsubscribe(String channel, int subscribedChannels) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onPUnsubscribe(String pattern, int subscribedChannels) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onPSubscribe(String pattern, int subscribedChannels) {
		// TODO Auto-generated method stub
	}

}
