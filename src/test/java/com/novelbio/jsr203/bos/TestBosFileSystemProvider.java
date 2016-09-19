package com.novelbio.jsr203.bos;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.GetObjectRequest;

public class TestBosFileSystemProvider {

	@Test
	public void testToBosPath() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetScheme() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewFileSystemURIMapOfStringQ() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetFileSystemURI() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetPathURI() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewByteChannelPathSetOfQextendsOpenOptionFileAttributeOfQArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewDirectoryStreamPathFilterOfQsuperPath() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCreateDirectoryPathFileAttributeOfQArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testDeletePath() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCopyPathPathCopyOptionArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testMovePathPathCopyOptionArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testIsSameFilePathPath() {
		//fail("Not yet implemented");
	}

	@Test
	public void testIsHiddenPath() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetFileStorePath() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCheckAccessPathAccessModeArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetFileAttributeViewPathClassOfVLinkOptionArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testReadAttributesPathClassOfALinkOptionArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testReadAttributesPathStringLinkOptionArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testSetAttributePathStringObjectLinkOptionArray() {
		//fail("Not yet implemented");
	}

	@Test
	public void testFileSystemProvider() {
		//fail("Not yet implemented");
	}

	@Test
	public void testInstalledProviders() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetScheme1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewFileSystemURIMapOfStringQ1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetFileSystemURI1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetPathURI1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewFileSystemPathMapOfStringQ() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewInputStream() {
		InputStream is = null;
		OutputStream os = null;
		BosClient client = null;
//		String file = "Dif_LncRNA_CSAvsCTL_1.5FC_FDR0.05.xls";
		String file = "small.txt";
//		String file = "arabidopsis_rna_2.fq";
//		String file2 = "q/DownFile2016-08-29.xls";
		String file2 = "fansTest/bigFile.fq";
		try {
			
			client = BosInitiator.getClient();
			client.putObject(PathDetail.getBucket(), file2, new File("/home/novelbio/data/" + file));
//			System.out.println("upload file start.");
//			long time1 = System.currentTimeMillis();
//			client.putObject(PathDetail.getBucket(), file2, new File("/home/novelbio/data/arabidopsis_rna_2.fq"));
//			long time2 = System.currentTimeMillis();
//			System.out.println("upload file. time=" + (time2 - time1));
			
//			URI uri = new URI("bos:///path/readFile");
			URI uri = new URI("bos:///" + file2);
			Path path = new BosFileSystemProvider().getPath(uri);
			is = Files.newInputStream(path);
			os = new FileOutputStream("/home/novelbio/tmp/" + file);
			byte[] buffer = new byte[5 * 1024 * 1024];
			int content;
			System.out.println("download 1 file start.");
			long time1 = System.currentTimeMillis();
			while ((content = is.read(buffer)) > -1) {
				os.write(buffer, 0, content);
			}
			long time2 = System.currentTimeMillis();
			System.out.println("download 1 file. time=" + (time2 - time1));
			System.out.println("finish");
			
			
			GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetail.getBucket(), file2);
			System.out.println("download 2 file start.");
			time1 = System.currentTimeMillis();
			client.getObject(getObjectRequest, new File("/home/novelbio/tmp/testfile.fq"));
			time2 = System.currentTimeMillis();
			System.out.println("download 2 file. time=" + (time2 - time1));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			close(is);
			close(os);
//			client.deleteObject(PathDetail.getBucket(), file2);
		}
		
	}

	@Test
	public void testNewOutputStream() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewFileChannel() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewAsynchronousFileChannel() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewByteChannelPathSetOfQextendsOpenOptionFileAttributeOfQArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testNewDirectoryStreamPathFilterOfQsuperPath1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCreateDirectoryPathFileAttributeOfQArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCreateSymbolicLink() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCreateLink() {
		//fail("Not yet implemented");
	}

	@Test
	public void testDeletePath1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testDeleteIfExists() {
		//fail("Not yet implemented");
	}

	@Test
	public void testReadSymbolicLink() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCopyPathPathCopyOptionArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testMovePathPathCopyOptionArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testIsSameFilePathPath1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testIsHiddenPath1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetFileStorePath1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testCheckAccessPathAccessModeArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testGetFileAttributeViewPathClassOfVLinkOptionArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testReadAttributesPathClassOfALinkOptionArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testReadAttributesPathStringLinkOptionArray1() {
		//fail("Not yet implemented");
	}

	@Test
	public void testSetAttributePathStringObjectLinkOptionArray1() {
		//fail("Not yet implemented");
	}

	public static void close(Closeable stream){
		try {
			if (stream != null) {
				stream.close();
			}
		} catch (Exception e) {
		}
	}
}
