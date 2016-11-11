package com.alibaba.dubbo.rpc.protocol.redis;

import com.alibaba.dubbo.common.extension.SPI;

import redis.clients.jedis.Jedis;

/**
 * 
 * @author fangzhou
 *
 */
@SPI("ark")
public interface RedisXSubscriberListener {
	public void onMessage(Jedis jedis, String channel, String messages);
}
