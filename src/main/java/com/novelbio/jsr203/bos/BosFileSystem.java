package com.novelbio.jsr203.bos;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.PutObjectResponse;

public class BosFileSystem extends FileSystem {
	private static final String DIR_SUFFIX = ".exist";
	
	BosClient client = BosInitiator.getClient();

	@Override
	public FileSystemProvider provider() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	final String getString(byte[] name) {
        return new String(name);
    }
	
	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isReadOnly() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getSeparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> supportedFileAttributeViews() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Path getPath(String first, String... more) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WatchService newWatchService() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterator<Path> iteratorOf(BosPath path,
			java.nio.file.DirectoryStream.Filter<? super Path> filter) throws IOException, URISyntaxException 
	{
		
		
		  //FileStatus inode = this.fs.getFileStatus(path.getRawResolvedPath());
        //if (inode.isDirectory() == false)
        //    throw new NotDirectoryException(getString(path.getResolvedPath()));
        List<Path> list = new ArrayList<Path>();
        for (FileStatus stat : this.fs.listStatus(path.getRawResolvedPath())) {
            HadoopPath hp = new HadoopPath(this, stat.getPath().toUri().getPath().getBytes());
            if (filter == null || filter.accept(hp))
                list.add(hp);
        }
        return list.iterator();
	}
	
	private void isFileExist() {
		
	}
	
	private boolean isDir(String absolutePathStr) {
		String[] bucket2Key = getBucket2Key(absolutePathStr);
		String key = addSep(bucket2Key[1]) + DIR_SUFFIX;
		try {
			client.getObject(bucket2Key[0], bucket2Key[1]);
			return true;
		} catch (BceServiceException e) {
			if (!"NoSuchKey".equals(e.getErrorCode())) {
				throw e;
			}
		}
		return false;
	}
	
	private boolean isDirEmpty(String absolutePathStr) {
		String[] bucket2Key = getBucket2Key(absolutePathStr);
		String key = addSep(bucket2Key[1]);
		try {
			client.(bucket2Key[0], bucket2Key[1]);
			return true;
		} catch (BceServiceException e) {
			if (!"NoSuchKey".equals(e.getErrorCode())) {
				throw e;
			}
		}
		return false;
	}
	
	/** 只能删除文件，不能删除文件夹 */
	protected void deleteFile(String absolutePathStr) {
		String[] bucket2Key = getBucket2Key(absolutePathStr);
		try {
			client.deleteObject(bucket2Key[0], bucket2Key[1]);
		} catch (BceServiceException e) {
			if (!"NoSuchKey".equals(e.getErrorCode())) {
				throw e;
			}
		}
	}
	
	protected void createDirectory(String path, FileAttribute<?>[] attrs) {
		String[] bucket2Key = getBucket2Key(path);
		String key = addSep(removeSplashHead(bucket2Key[1], false)) + DIR_SUFFIX;
		client.putObject(bucket2Key[0], key, new byte[]{1});
	}
	
	/**
	 * 输入类似 bos:/novelbio/myfile/test.txt
	 * 类型
	 * novelbio是bucket
	 * myfile/test.txt 是 key
	 * @param path
	 * @return
	 */
	private String[] getBucket2KeyWithBucket(String path) {
		path = removeSplashHead(path, false);
		String uploadPath = path.replaceFirst(BosFileSystemProvider.SCHEME + ":", "");
		uploadPath = removeSplashHead(uploadPath, false);
		String[] bucketName = uploadPath.split("/+");
		String bucket = bucketName[0];
		String key = uploadPath.replaceFirst(bucket, "");
		return new String[]{bucket, key};
	}
	
	/**
	 * 输入类似 bos:/myfile/test.txt
	 * 类型
	 * novelbio是bucket
	 * myfile/test.txt 是 key
	 * @param path
	 * @return
	 */
	private String[] getBucket2Key(String path) {
		path = removeSplashHead(path, false);
		String uploadPath = path.replaceFirst(BosFileSystemProvider.SCHEME + ":", "");
		uploadPath = removeSplashHead(uploadPath, false);
		return new String[]{PathDetail.getBucket(), uploadPath};
	}
	
	/** 将文件开头的"//"这种多个的去除
	 * @param fileName
	 * @param keepOne 是否保留一个“/”
	 * @return
	 */
	private static String removeSplashHead(String fileName, boolean keepOne) {
		String head = "//";
		if (!keepOne) {
			head = "/";
		}
		String fileNameThis = fileName;
		while (true) {
			if (fileNameThis.startsWith(head)) {
				fileNameThis = fileNameThis.substring(1);
			} else {
				break;
			}
		}
		return fileNameThis;
	}
	
	/**
	 * 添加文件分割符
	 * 
	 * @param path
	 * @return
	 */
	public static String addSep(String path) {
		path = path.trim();
		if (!path.endsWith(File.separator)) {
			if (!path.equals("")) {
				path = path + File.separator;
            }
		}
		return path;
	}


}
