package com.novelbio.jsr203.bos;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.AccessMode;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.baidubce.BceServiceException;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;

/**
 * 百度bos文件系统
 * 
 * @author novelbio
 *
 */
public class BosFileSystem extends FileSystem {
	private static final String DIR_SUFFIX = ".exist";

	private boolean readOnly;
	private volatile boolean isOpen = true;
	FileSystemProvider fileSystemProvider;
	FileSystem fileSystem;
	BosClient client = BosInitiator.getClient();

	public BosFileSystem(FileSystemProvider fileSystemProvider) {
		this.fileSystemProvider = fileSystemProvider;
		this.fileSystem = new BosFileSystem(fileSystemProvider);
	}
	
	@Override
	public FileSystemProvider provider() {
		return fileSystemProvider;
	}

	@Override
	public void close() throws IOException {
	}

	final String getString(byte[] name) {
		return new String(name);
	}

	@Override
	public boolean isOpen() {
		return isOpen;
	}

	@Override
	public boolean isReadOnly() {
		return readOnly;
	}

	@Override
	public String getSeparator() {
		return "/";
	}

	@Override
	public Iterable<Path> getRootDirectories() {
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(PathDetail.getBucket());
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		// 列出根目录下的所有文件和文件夹
		listObjectsRequest.setPrefix("");
		ListObjectsResponse lsObject = client.listObjects(listObjectsRequest);
		List<Path> lsPaths = new ArrayList<>();
		if (lsObject.getContents() != null) {
			// 文件
			lsObject.getContents().forEach(bos -> lsPaths.add(new BosPath(fileSystem, bos.getKey().getBytes())));
		}
		if (lsObject.getCommonPrefixes() != null) {
			// 文件夹
			lsObject.getCommonPrefixes().forEach(prefix -> lsPaths.add(new BosPath(fileSystem, prefix.getBytes())));
		}
		return lsPaths;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		ArrayList<FileStore> list = new ArrayList<>(1);
		list.add(new BosFileStore(new BosPath(this, new byte[] { '/' })));
		return list;
	}

	private static final Set<String> supportedFileAttributeViews = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("bos")));
	
	@Override
	public Set<String> supportedFileAttributeViews() {
		return supportedFileAttributeViews;
	}

	@Override
	public Path getPath(String first, String... more) {
		String path;
		if (more.length == 0) {
			path = first;
		} else {
			StringBuilder sb = new StringBuilder();
			sb.append(first);
			for (String segment : more) {
				if (segment.length() > 0) {
					if (sb.length() > 0) sb.append('/');
					sb.append(segment);
				}
			}
			path = sb.toString();
		}
		return new BosPath(this, path.getBytes());
	}

	private static final String GLOB_SYNTAX = "glob";
	private static final String REGEX_SYNTAX = "regex";
	
	@Override
	public PathMatcher getPathMatcher(String syntaxAndPattern) {
		int pos = syntaxAndPattern.indexOf(':');
		if (pos <= 0 || pos == syntaxAndPattern.length()) {
			throw new IllegalArgumentException();
		}
		String syntax = syntaxAndPattern.substring(0, pos);
		String input = syntaxAndPattern.substring(pos + 1);
		String expr;
		if (syntax.equals(GLOB_SYNTAX)) {
			expr = RegexUtil.toRegexPattern(input);
		} else {
			if (syntax.equals(REGEX_SYNTAX)) {
				expr = input;
			} else {
				throw new UnsupportedOperationException("Syntax '" + syntax + "' not recognized");
			}
		}
		// return matcher
		final Pattern pattern = Pattern.compile(expr);
		return new PathMatcher() {
			@Override
			public boolean matches(Path path) {
				return pattern.matcher(path.toString()).matches();
			}
		};
	}

	@Override
	public UserPrincipalLookupService getUserPrincipalLookupService() {
//		return null;
		throw new RuntimeException("this method is unsuppored.");
	}

	/* 文件监听服务,暂可不实现
	 * @see java.nio.file.FileSystem#newWatchService()
	 */
	@Override
	public WatchService newWatchService() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterator<Path> iteratorOf(BosPath path, java.nio.file.DirectoryStream.Filter<? super Path> filter) throws IOException, URISyntaxException {

		// FileStatus inode = this.fs.getFileStatus(path.getRawResolvedPath());
		// if (inode.isDirectory() == false)
		// throw new NotDirectoryException(getString(path.getResolvedPath()));
		List<Path> list = new ArrayList<Path>();
		// for (FileStatus stat : this.fs.listStatus(path.getRawResolvedPath()))
		// {
		// HadoopPath hp = new HadoopPath(this,
		// stat.getPath().toUri().getPath().getBytes());
		// if (filter == null || filter.accept(hp))
		// list.add(hp);
		// }
		return list.iterator();
	}

	public BosClient getBos() {
		return this.client;
	}
	
//	private void isFileExist() {
//
//	}
//
//	private boolean isDir(String absolutePathStr) {
//		String[] bucket2Key = getBucket2Key(absolutePathStr);
//		String key = addSep(bucket2Key[1]) + DIR_SUFFIX;
//		try {
//			client.getObject(bucket2Key[0], bucket2Key[1]);
//			return true;
//		} catch (BceServiceException e) {
//			if (!"NoSuchKey".equals(e.getErrorCode())) {
//				throw e;
//			}
//		}
//		return false;
//	}
//
//	private boolean isDirEmpty(String absolutePathStr) {
//		String[] bucket2Key = getBucket2Key(absolutePathStr);
//		String key = addSep(bucket2Key[1]);
//		try {
//			// client.(bucket2Key[0], bucket2Key[1]);
//			return true;
//		} catch (BceServiceException e) {
//			if (!"NoSuchKey".equals(e.getErrorCode())) {
//				throw e;
//			}
//		}
//		return false;
//	}

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
		client.putObject(bucket2Key[0], key, new byte[] { 1 });
	}

	/**
	 * 输入类似 bos:/novelbio/myfile/test.txt 类型 novelbio是bucket myfile/test.txt 是
	 * key
	 * 
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
		return new String[] { bucket, key };
	}

	/**
	 * 输入类似 bos:/myfile/test.txt 类型 novelbio是bucket myfile/test.txt 是 key
	 * 
	 * @param path
	 * @return
	 */
	private String[] getBucket2Key(String path) {
		path = removeSplashHead(path, false);
		String uploadPath = path.replaceFirst(BosFileSystemProvider.SCHEME + ":", "");
		uploadPath = removeSplashHead(uploadPath, false);
		return new String[] { PathDetail.getBucket(), uploadPath };
	}

	/**
	 * 将文件开头的"//"这种多个的去除
	 * 
	 * @param fileName
	 * @param keepOne
	 *            是否保留一个“/”
	 * @return
	 */
	private static String removeSplashHead(String fileName, boolean keepOne) {
		String head = "//";
		if (!keepOne) {
			head = "/";
		}
		String fileNameThis = fileName;
		while (fileName.startsWith(head)) {
			fileName = fileName.substring(1);
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
		if (!path.equals("") && !path.endsWith(File.separator)) {
			path = path + File.separator;
		}
		return path;
	}
//
//	public void moveFile(String absolutePathStr, String absolutePathStr2, CopyOption[] options) {
//		client.
//	}
//
//	public void copyFile(boolean b, byte[] resolvedPath, byte[] resolvedPath2, CopyOption[] options) {
//		client.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey)
//	}
//
//	public boolean exists(Path rawResolvedPath) throws IOException{
//		// TODO Auto-generated method stub
//		return false;
//	}

	public void checkAccess(BosPath bosPath, AccessMode[] modes) {
		// TODO Auto-generated method stub
		
	}

	public <V extends FileAttributeView> V getView(BosPath bosPath, Class<V> type) {
		// TODO Auto-generated method stub
		return null;
	}

}
