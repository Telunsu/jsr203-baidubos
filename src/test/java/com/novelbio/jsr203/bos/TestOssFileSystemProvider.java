package com.novelbio.jsr203.bos;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.google.common.collect.Sets;

public class TestOssFileSystemProvider {
	
	OSSClient client = OssInitiator.getClient();

	@Test
	public void testToBosPath() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			OssPath ossPath = new OssFileSystemProvider().toBosPath(path);
			Assert.assertNotNull(ossPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetScheme() {
		Assert.assertEquals("http", new OssFileSystemProvider().getScheme());
	}

	@Test
	public void testNewFileSystemURIMapOfStringQ() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			FileSystem fileSystem = new OssFileSystemProvider().newFileSystem(uri, new HashMap<>());
			Assert.assertTrue(fileSystem instanceof OssFileSystem);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetFileSystemURI() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			FileSystem fileSystem =new OssFileSystemProvider().getFileSystem(uri);
			Assert.assertTrue(fileSystem instanceof OssFileSystem);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetPathURI() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path =new OssFileSystemProvider().getPath(uri);
			Assert.assertTrue(path instanceof OssPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testNewByteChannelPathSetOfQextendsOpenOptionFileAttributeOfQArray() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path =new OssFileSystemProvider().getPath(uri);
			SeekableByteChannel channel = new OssFileSystemProvider().newByteChannel(path, Sets.newHashSet(StandardOpenOption.READ), null);
			ByteBuffer bytebuffer = ByteBuffer.allocate(128);
			channel.read(bytebuffer);
			String content = new String(bytebuffer.array());
			Assert.assertTrue(content.length() > 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testNewDirectoryStreamPathFilterOfQsuperPath() {
		try {
			String file2 = "novelbio/";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			DirectoryStream<Path> directoryStream = new OssFileSystemProvider().newDirectoryStream(path, null);
			if (directoryStream != null) {
				directoryStream.forEach(p -> System.out.println(p.getFileName()));
			}

			DirectoryStream<Path> directoryStream2 = Files.newDirectoryStream(path);
			if (directoryStream2 != null) {
				directoryStream2.forEach(p -> System.out.println(p.getFileName()));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCreateDirectoryPathFileAttributeOfQArray() {
		try {
			String file2 = "novelbio/folderTest";
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			client.deleteObject(PathDetail.getBucket(), file2 + "/");
			new OssFileSystemProvider().createDirectory(path, null);
			
			Assert.assertTrue(client.doesObjectExist(PathDetail.getBucket(), file2 + "/"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeletePath() {
		String file = "small.txt";
		try {
			client.putObject(PathDetail.getBucket(), file, new ByteArrayInputStream(new byte[]{}));
			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file);
			Files.delete(new OssFileSystemProvider().getPath(uri));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCopyPathPathCopyOptionArray() {
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "dataFile.txt";
			String file2 = "dataFile_copy.txt";
			URI uri1 = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file1);
			Path path1 = provider.getPath(uri1);
			URI uri2 = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			client.putObject(PathDetail.getBucket(), file1, new File("/home/novelbio/data/small.txt"));
			
			Files.copy(path1, path2, StandardCopyOption.COPY_ATTRIBUTES);
			Assert.assertTrue(client.doesObjectExist(PathDetail.getBucket(), path2.toString()));
			Files.delete(path2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "/home/novelbio/data/small.txt";
			String file2 = "dataFile_copy2.txt";
			Path path1 = new File(file1).toPath();
			URI uri2 = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.copy(path1, path2, StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue(client.doesObjectExist(PathDetail.getBucket(), path2.toString()));
//			Files.delete(path2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Test
	public void testMovePathPathCopyOptionArray() {
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "dataFile.txt";
			String file2 = "dataFile_move.txt";
			URI uri1 = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file1);
			Path path1 = provider.getPath(uri1);
			URI uri2 = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.move(path1, path2, StandardCopyOption.COPY_ATTRIBUTES);
			
			Assert.assertTrue(client.doesObjectExist(PathDetail.getBucket(), file2));
			
			Files.move(path2, path1, StandardCopyOption.COPY_ATTRIBUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "/home/novelbio/data/small.txt";
			String file2 = "dataFile_move2.txt";
			Path path1 = new File(file1).toPath();
			URI uri2 = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.move(path1, path2, StandardCopyOption.COPY_ATTRIBUTES);
			
			Assert.assertTrue(client.doesObjectExist(PathDetail.getBucket(), file2));
			
			Files.move(path2, path1, StandardCopyOption.COPY_ATTRIBUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
//		InputStream is = null;
//		OutputStream os = null;
//		OSSClient client = null;
//		String file = "small.txt";
//		String file2 = "test/dir/exist";
//		try {
//			client = OssInitiator.getClient();
//			client.putObject(PathDetail.getBucket(), file2, new File("/home/novelbio/data/" + file));
//			System.out.println("upload file start.");
//			long time1 = System.currentTimeMillis();
////			client.putObject(PathDetail.getBucket(), file2, new File("/home/novelbio/data/arabidopsis_rna_2.fq"));
//			long time2 = System.currentTimeMillis();
//			System.out.println("upload file. time=" + (time2 - time1));
//			
////			URI uri = new URI("bos:///path/readFile");
//			URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + file2);
//			Path path = new OssFileSystemProvider().getPath(uri);
//			is = Files.newInputStream(path);
//			os = new FileOutputStream("/home/novelbio/tmp/" + file);
//			byte[] buffer = new byte[5 * 1024 * 1024];
//			int content;
//			System.out.println("download 1 file start.");
//			time1 = System.currentTimeMillis();
////			while ((content = is.read(buffer)) > -1) {
////				os.write(buffer, 0, content);
////			}
//			time2 = System.currentTimeMillis();
//			System.out.println("download 1 file. time=" + (time2 - time1));
//			System.out.println("finish");
//			
//			
//			GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetail.getBucket(), file2);
//			System.out.println("download 2 file start.");
//			time1 = System.currentTimeMillis();
//			client.getObject(getObjectRequest, new File("/home/novelbio/tmp/testfile.fq"));
//			time2 = System.currentTimeMillis();
//			System.out.println("download 2 file. time=" + (time2 - time1));
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			close(is);
//			close(os);
//		}
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
			String ossFileName = "dataFile.txt"	;
			File file = new File("/home/novelbio/data/small.txt");
			is = Files.newInputStream(file.toPath());
			Path path = new OssFileSystemProvider().getPath(new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + ossFileName));
			os = Files.newOutputStream(path, StandardOpenOption.CREATE);
			byte[] buffer = new byte[128];
			int len;
			while ((len = is.read(buffer)) > 0) {
				os.write(buffer, 0, len);
			}
			
			Assert.assertTrue(client.doesObjectExist(PathDetail.getBucket(), ossFileName));
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				System.out.println(e.getMessage());
			}
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
