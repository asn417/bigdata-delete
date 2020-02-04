package com.asn.bigdata;

import com.asn.bigdata.hadoop.HDFSUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BigDataApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	void existFile() throws Exception {
		System.out.println(HDFSUtils.existFile("/user/wangsen/bbb"));
	}
}
