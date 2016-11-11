package com.alibaba.dubbo.rpc.protocol.redis;

import redis.clients.jedis.Jedis;

public class MockRedisXSubscriberListener implements RedisXSubscriberListener {

	@Override
	public void onMessage(Jedis jedis, String channel, String messages) {
		// TODO Auto-generated method stub
	}

}
