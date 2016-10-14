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

import org.assertj.core.api.SoftAssertions;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectListing;
import com.google.common.collect.Sets;

public class TestOssFileSystemProvider {
	
	OSSClient client = OssInitiator.getClient();

	@Test
	public void testToBosPath() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			OssPath ossPath = OssFileSystemProvider.toBosPath(path);
			Assert.assertNotNull(ossPath);
			Assert.assertTrue(ossPath.toString().equals(file2));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			String file2 = "small.txt";
			URI uri = new URI("oss://" + OssConfig.getBucket() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			OssPath ossPath = OssFileSystemProvider.toBosPath(path);
			Assert.assertNotNull(ossPath);
			Assert.assertTrue(ossPath.toString().equals(file2));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			// 不支持一个斜杠的
			String file2 = "small.txt";
			URI uri = new URI("oss:/" + OssConfig.getBucket() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			OssPath ossPath = OssFileSystemProvider.toBosPath(path);
			Assert.assertNotNull(ossPath);
			Assert.assertFalse(ossPath.toString().equals(file2));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			// 不支持一个斜杠的
			String file2 = "small.txt";
			URI uri = new URI("oss:/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			OssPath ossPath = OssFileSystemProvider.toBosPath(path);
			Assert.assertNotNull(ossPath);
			Assert.assertTrue(ossPath.toString().equals(file2));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetScheme() {
		Assert.assertEquals("oss", new OssFileSystemProvider().getScheme());
	}

	@Test
	public void testNewFileSystemURIMapOfStringQ() {
		try {
			String file2 = "small.txt";
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
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
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
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
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
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
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path =new OssFileSystemProvider().getPath(uri);
			SeekableByteChannel channel = new OssFileSystemProvider().newByteChannel(path, Sets.newHashSet(StandardOpenOption.READ));
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
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
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
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path = new OssFileSystemProvider().getPath(uri);
			client.deleteObject(PathDetailOs.getBucket(), file2 + "/");
			new OssFileSystemProvider().createDirectory(path, null);
			
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), file2 + "/"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDeletePath() {
		String file = "small.txt";
		try {
			client.putObject(PathDetailOs.getBucket(), file, new ByteArrayInputStream(new byte[]{}));
			URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + file);
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
			URI uri1 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file1);
			Path path1 = provider.getPath(uri1);
			URI uri2 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			client.putObject(PathDetailOs.getBucket(), file1, new File("/home/novelbio/data/small.txt"));
			
			Files.copy(path1, path2, StandardCopyOption.COPY_ATTRIBUTES);
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), path2.toString()));
			Files.delete(path2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "/home/novelbio/data/small.txt";
			String file2 = "dataFile_copy2.txt";
			Path path1 = new File(file1).toPath();
			URI uri2 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.copy(path1, path2, StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), path2.toString()));
			Files.delete(path2);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "/home/novelbio/data/small.txt";
			String file2 = "dataFile_copy2.txt";
			Path path1 = new File(file1).toPath();
			URI uri2 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.copy(path2, path1, StandardCopyOption.REPLACE_EXISTING);
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), path2.toString()));
			Files.delete(path2);
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
			URI uri1 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file1);
			Path path1 = provider.getPath(uri1);
			URI uri2 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.move(path1, path2, StandardCopyOption.COPY_ATTRIBUTES);
			
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), file2));
			
			Files.move(path2, path1, StandardCopyOption.COPY_ATTRIBUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "/home/novelbio/data/small.txt";
			String file2 = "dataFile_move2.txt";
			Path path1 = new File(file1).toPath();
			URI uri2 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.move(path1, path2, StandardCopyOption.COPY_ATTRIBUTES);
			
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), file2));
			
			Files.move(path2, path1, StandardCopyOption.COPY_ATTRIBUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider provider = new OssFileSystemProvider();
			String file1 = "/home/novelbio/data/small2.txt";
			String file2 = "dataFile.txt";
			Path path1 = new File(file1).toPath();
			URI uri2 = new URI("oss://" + PathDetailOs.getBucket() + "/" + file2);
			Path path2 = provider.getPath(uri2);
			
			Files.move(path2, path1, StandardCopyOption.COPY_ATTRIBUTES);
			
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), file2));
			
			Files.move(path2, path1, StandardCopyOption.COPY_ATTRIBUTES);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCheckAccessPathAccessModeArray() {

		try {
			OssFileSystemProvider ossFileSystemProvider = new OssFileSystemProvider();
			String path = "oss:/nbCloud/public/AllProject/A__2016-09/project_57ea175c45ce95f1d60f8af5/task_57ea391d45ceed1b0a58cee8/";
			Path ossPath = ossFileSystemProvider.getPath(new URI(path));
			ossFileSystemProvider.checkAccess(ossPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider ossFileSystemProvider = new OssFileSystemProvider();
			String path = "oss:/nbCloud/public/AllProject/A__2016-09/project_57ea175c45ce95f1d60f8af5/task_57ea391d45ceed1b0a58cee8";
			Path ossPath = ossFileSystemProvider.getPath(new URI(path));
			ossFileSystemProvider.checkAccess(ossPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider ossFileSystemProvider = new OssFileSystemProvider();
			String path = "oss:/nbCloud/public/AllProject/A__2016-09/project_57ea175c45ce95f1d60f8af5/task_57ea391d45ceed1b0a58cee8/";
			Path ossPath = ossFileSystemProvider.getPath(new URI(path));
			Files.createDirectories(ossPath);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			OssFileSystemProvider ossFileSystemProvider = new OssFileSystemProvider();
			String path = "oss:/nbCloud/public/AllProject2/A__2016-09/project_57ea175c45ce95f1d60f8af5/task_57ea391d45ceed1b0a58cee8/";
			Path ossPath = ossFileSystemProvider.getPath(new URI(path));
			ossFileSystemProvider.checkAccess(ossPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void tetWrite() {
		
		InputStream is = null;
		OutputStream os = null;
		try {
			String ossFileName = "dataFile.txt"	;
			client.deleteObject(PathDetailOs.getBucket(), ossFileName);
			File file = new File("/home/novelbio/git/jsr203-aliyun/src/test/resources/testFile/big.bam");
			is = Files.newInputStream(file.toPath());
			Path path = new OssFileSystemProvider().getPath(new URI("oss://" + PathDetailOs.getBucket() + "/" + ossFileName));
			os = Files.newOutputStream(path, StandardOpenOption.CREATE);
			byte[] buffer = new byte[128];
			int len;
			while ((len = is.read(buffer)) > 0) {
				os.write(buffer, 0, len);
			}
			closeStream(is);
			closeStream(os);
			
			Assert.assertTrue(client.doesObjectExist(PathDetailOs.getBucket(), ossFileName));
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				System.out.println(e.getMessage());
			}
		} finally {
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

	public static void close(Closeable stream){
		try {
			if (stream != null) {
				stream.close();
			}
		} catch (Exception e) {
		}
	}
}
