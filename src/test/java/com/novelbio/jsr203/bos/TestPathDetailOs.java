package com.novelbio.jsr203.bos;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestPathDetailOs {
	

	@Test
	public void testGetRegionId() {
		String res = PathDetailOs.getRegionId();
		assertNotNull(res);
	}

	@Test
	public void testGetImageId() {
		String res = PathDetailOs.getImageId();
		assertNotNull(res);
	}

	@Test
	public void testGetInstanceType() {
		String res = PathDetailOs.getInstanceType();
		assertNotNull(res);
	}

	@Test
	public void testGetVmCounts() {
		int res = PathDetailOs.getVmCounts();
		assertTrue(res > 0);
	}

	@Test
	public void testGetDockerRegistry() {
		String res = PathDetailOs.getDockerRegistry();
		assertNotNull(res);
	}

	@Test
	public void testGetClientObjectSaveService() {
		String res = PathDetailOs.getClientObjectSaveService();
		assertNotNull(res);
	}

	@Test
	public void testGetClientStsDomain() {
		String res = PathDetailOs.getClientStsDomain();
		assertNotNull(res);
	}

	@Test
	public void testGetClientAccessKeyID() {
		String res = PathDetailOs.getClientAccessKeyID();
		assertNotNull(res);
	}

	@Test
	public void testGetClientAccessKeySecret() {
		String res = PathDetailOs.getClientAccessKeySecret();
		assertNotNull(res);
	}

	@Test
	public void testGetClientStsApiVersion() {
		String res = PathDetailOs.getClientStsApiVersion();
		assertNotNull(res);
	}

	@Test
	public void testGetClientRegionId() {
		String res = PathDetailOs.getClientRegionId();
		assertNotNull(res);
	}

	@Test
	public void testGetClientRoleArn() {
		String res = PathDetailOs.getClientRoleArn();
		assertNotNull(res);
	}

	@Test
	public void testGetClientEndpoint() {
		String res = PathDetailOs.getClientEndpoint();
		assertNotNull(res);
	}

	@Test
	public void testGetClientBucket() {
		String res = PathDetailOs.getClientBucket();
		assertNotNull(res);
	}

	@Test
	public void testGetBucket() {
		String res = PathDetailOs.getBucket();
		assertNotNull(res);
	}

	@Test
	public void testGetAccessKey() {
		String res = PathDetailOs.getAccessKey();
		assertNotNull(res);
	}

	@Test
	public void testGetAccessKeySecret() {
		String res = PathDetailOs.getAccessKeySecret();
		assertNotNull(res);
	}

	@Test
	public void testGetEndpoint() {
		String res = PathDetailOs.getEndpoint();
		assertNotNull(res);
	}

	@Test
	public void testGetOsSymbol() {
		String res = PathDetailOs.getOsSymbol();
		assertNotNull(res);
	}

	@Test
	public void testGetOsMountPathWithSep() {
		String res = PathDetailOs.getOsMountPathWithSep();
		assertNotNull(res);
	}

	

}
