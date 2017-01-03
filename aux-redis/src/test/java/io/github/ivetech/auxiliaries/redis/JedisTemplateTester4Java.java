package io.github.ivetech.auxiliaries.redis;

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
        jedisTemplate.setHost("192.168.20.30");
        jedisTemplate.setPort(6378);
        jedisTemplate.setCheck(true);
        
        // init 
        jedisTemplate.init();

        // do sth.
        Assert.assertTrue(jedisTemplate.set("B", "B"));

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals("B", jedisTemplate.get("B"));    
        }

        // destroy 
        jedisTemplate.destory();
    }


    @Test
    public void testSentinelsMode () {
        JedisTemplate jedisTemplate = new JedisTemplate();

        jedisTemplate.setStartupMode(JedisTemplate.StartupMode.sentinels);
        jedisTemplate.setCheck(true);
        jedisTemplate.setMasters("master001");
        jedisTemplate.setMaxWaitMillis(-1);
        jedisTemplate.setMaxTotal(500);
        jedisTemplate.setSentinels("192.168.20.32:26378,192.168.20.32:36378,192.168.20.32:46378");
        jedisTemplate.init();
long start = System.currentTimeMillis();
        
        for (int i = 0; i < 10000; i++) {
            jedisTemplate.set("G6418P16088752992256" + i ,"Be7xHOeqjG9hXwLgPqCkit1XUQi4pISpvSgtuGA2IKY6ZatRtfCfXg==" + i);    
        }
        System.out.println(System.currentTimeMillis() - start);
//        Assert.assertTrue(jedisTemplate.set("BB", "BB"));
//        Assert.assertEquals("BB", jedisTemplate.get("BB"));

        jedisTemplate.destory();
    }
}
