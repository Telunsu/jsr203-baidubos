package com.novelbio.jsr203.bos;

import java.awt.image.BufferedImage;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import javax.imageio.ImageIO;

import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.GetObjectRequest;

public class TestOssFileSystemProvider {

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
		OSSClient client = null;
		String file = "small.txt";
		String file2 = "test/dir/exist";
		try {
			client = OssInitiator.getClient();
			client.putObject(PathDetail.getBucket(), file2, new File("/home/novelbio/data/" + file));
			System.out.println("upload file start.");
			long time1 = System.currentTimeMillis();
//			client.putObject(PathDetail.getBucket(), file2, new File("/home/novelbio/data/arabidopsis_rna_2.fq"));
			long time2 = System.currentTimeMillis();
			System.out.println("upload file. time=" + (time2 - time1));
			
//			URI uri = new URI("bos:///path/readFile");
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			is = Files.newInputStream(path);
			os = new FileOutputStream("/home/novelbio/tmp/" + file);
			byte[] buffer = new byte[5 * 1024 * 1024];
			int content;
			System.out.println("download 1 file start.");
			time1 = System.currentTimeMillis();
//			while ((content = is.read(buffer)) > -1) {
//				os.write(buffer, 0, content);
//			}
			time2 = System.currentTimeMillis();
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
		}
	}
	
	@Test
	public void tetWrite() {
//		try {
//			File file = new File("/home/novelbio/桌面/首页项目9.png");
//			BufferedImage img = ImageIO.read(file);
//			String file2 = "首页项目9.png";
//			OSSClient client = OssInitiator.getClient();
//			client.deleteObject(PathDetail.getBucket(), file2);
//			Path path = new OssFileSystemProvider().getPath(new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2));
//			OutputStream outStream = Files.newOutputStream(path, StandardOpenOption.CREATE);
//			ImageIO.write(img, "jpg", outStream);
//			
//			outStream.flush();
//			outStream.close();
//			img.flush();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
		InputStream is = null;
		OutputStream os = null;
		try {
			File file = new File("/home/novelbio/data/arabidopsis_rna_2.fq");
			is = Files.newInputStream(file.toPath());
			Path path = new OssFileSystemProvider().getPath(new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/dataFile.txt"));
			os = Files.newOutputStream(path, StandardOpenOption.CREATE);
			byte[] buffer = new byte[128];
			int len;
			while ((len = is.read(buffer)) > 0) {
				os.write(buffer, 0, len);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeStream(is);
			closeStream(os);
		}
	}

	private void closeStream(Closeable close) {
		if (close != null) {
			try {
				close.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
