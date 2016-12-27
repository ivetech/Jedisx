package xyz.vopen.auxiliaries.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

/**
 * com.pyw.commons.cache.redis
 *
 * @author Elve.xu [xuhw@yyft.com]
 * @version v1.0 - 27/12/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:application.xml")
public class JedisTemplateTester4Spring {

    @Resource
    JedisTemplate jedisTemplate;

    @Test
    public void main () {
      
        System.out.println("Set A Result : " + jedisTemplate.set("A", "A"));

        System.out.println("Get A Result : " + jedisTemplate.get("A"));

    }

}
