package xyz.vopen.auxiliaries.redis;

import org.junit.Assert;
import org.junit.Test;

/**
 * xyz.vopen.auxiliaries.redis
 *
 * @author Elve.xu [iskp.me@gmail.com]
 * @version v1.0 - 27/12/2016.
 */
public class JedisTemplateTester4Java {


    @Test
    public void testSingleMode () {

        // new instance
        JedisTemplate jedisTemplate = new JedisTemplate();

        // set params
        jedisTemplate.setStartupMode(JedisTemplate.StartupMode.single);
        jedisTemplate.setHost("192.168.20.75");
        jedisTemplate.setPort(6379);
        jedisTemplate.setCheck(true);

        // init 
        jedisTemplate.init();

        // do sth.
        Assert.assertTrue(jedisTemplate.set("B", "B"));
        Assert.assertEquals("B", jedisTemplate.get("B"));

        // destroy 
        jedisTemplate.destory();
    }


    @Test
    public void testSentinelsMode () {
        JedisTemplate jedisTemplate = new JedisTemplate();

        jedisTemplate.setStartupMode(JedisTemplate.StartupMode.sentinels);
        jedisTemplate.setCheck(false);
        jedisTemplate.setMasters("master001");
        jedisTemplate.setSentinels("192.168.20.32:26378,192.168.20.32:36378,192.168.20.32:46378");
        jedisTemplate.init();

        Assert.assertTrue(jedisTemplate.set("BB", "BB"));
        Assert.assertEquals("BB", jedisTemplate.get("BB"));

        jedisTemplate.destory();
    }
}
