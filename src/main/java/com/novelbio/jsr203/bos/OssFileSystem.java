package com.novelbio.jsr203.bos;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessMode;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
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

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.UploadPartRequest;

/**
 * 阿里云oss文件系统
 * 
 * @author novelbio
 *
 */
public class OssFileSystem extends FileSystem {
	private static final String DIR_SUFFIX = ".exist";

	private boolean readOnly;
	private volatile boolean isOpen = true;
	FileSystemProvider fileSystemProvider;
//	FileSystem fileSystem;
	OSSClient client = OssInitiator.getClient();

	public OssFileSystem(FileSystemProvider fileSystemProvider) {
		this.fileSystemProvider = fileSystemProvider;
//		this.fileSystem = new BosFileSystem(fileSystemProvider);
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
		ObjectListing lsObject = client.listObjects(listObjectsRequest);
		List<Path> lsPaths = new ArrayList<>();
		if (lsObject.getObjectSummaries() != null) {
			// 文件
			lsObject.getObjectSummaries().forEach(bos -> lsPaths.add(new OssPath(this, bos.getKey().getBytes())));
		}
		if (lsObject.getCommonPrefixes() != null) {
			// 文件夹
			lsObject.getCommonPrefixes().forEach(prefix -> lsPaths.add(new OssPath(this, prefix.getBytes())));
		}
		return lsPaths;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		ArrayList<FileStore> list = new ArrayList<>(1);
		list.add(new OssFileStore(new OssPath(this, new byte[] { '/' })));
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
		
		while (path.startsWith("/")) {
			path = path.substring(1);
		}
		return new OssPath(this, path.getBytes());
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

	/**
	 * 获取路径下的文件夹和文件
	 * 
	 * @param ossPath
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	public Iterator<Path> iteratorOf(OssPath ossPath, java.nio.file.DirectoryStream.Filter<? super Path> filter) throws Exception {
		List<Path> lsOssPath = new ArrayList<>();
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(PathDetail.getBucket());
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		// 列出ossPath目录下的内容
		listObjectsRequest.setPrefix(ossPath.toString());
		ObjectListing objectListing = OssInitiator.getClient().listObjects(listObjectsRequest);

		if (objectListing.getCommonPrefixes() != null) {
			for (String name : objectListing.getCommonPrefixes()) {
				URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + name);
				lsOssPath.add(fileSystemProvider.getPath(uri));
			}
		}

		if (objectListing.getObjectSummaries() != null) {
			for (OSSObjectSummary summary : objectListing.getObjectSummaries()) {
				if (summary.getKey().equals(ossPath.toString())) {
					// 默认返回内容中有一个ossPath,这个不需要.过滤掉.
					continue;
				}
				URI uri = new URI("http://" + PathDetail.getBucket() + "." + PathDetail.getEndpoint() + "/" + summary.getKey());
				lsOssPath.add(fileSystemProvider.getPath(uri));
			}
		}
		return lsOssPath.iterator();
	}

	public OSSClient getBos() {
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
	protected void deleteFile(Path path) {
//		String[] bucket2Key = getBucket2Key(absolutePathStr);
		try {
			// TODO 删除文件,需判断是不是一个文件,不能把文件夹删除了.
			if (client.doesObjectExist(PathDetail.getBucket(), path.toString())) {
				client.deleteObject(PathDetail.getBucket(), path.toString());
			}
		} catch (OSSException|ClientException e) {
			throw e;
		}
	}

	protected void createDirectory(String path, FileAttribute<?>[] attrs) {
		if(!path.endsWith("/")) {
			path = path + "/";
		}
		
		if (client.doesObjectExist(PathDetail.getBucket(), path)) {
			throw new RuntimeException("path is existed.path=" + path);
		}
		
		client.putObject(PathDetail.getBucket(), path, new ByteArrayInputStream(new byte[]{}));
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
		String uploadPath = path.replaceFirst(OssFileSystemProvider.SCHEME + ":", "");
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
		String uploadPath = path.replaceFirst(OssFileSystemProvider.SCHEME + ":", "");
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


	public void checkAccess(OssPath bosPath, AccessMode[] modes) {
		// TODO Auto-generated method stub
		
	}

	public <V extends FileAttributeView> V getView(OssPath bosPath, Class<V> type) {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream newInputStream(Path path, OpenOption[] options) {
		GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetail.getBucket(), path.toString());
		OSSObject bosObject = client.getObject(getObjectRequest);
		return bosObject.getObjectContent();
	}

	public DirectoryStream<Path> newDirectoryStream(OssPath ossPath, Filter<? super Path> filter) throws IOException {
		return new OssDirectoryStream(ossPath, filter);
	}

	public void copy(OssPath source, OssPath target) {
		if (!client.doesObjectExist(PathDetail.getBucket(), source.toString())) {
			throw new RuntimeException("source file not exist! source=" + source);
		}
		if (client.doesObjectExist(PathDetail.getBucket(), target.toString())) {
			throw new RuntimeException("target file exist! target=" + target);
		}
		
		OSSObject ossObject = client.getObject(PathDetail.getBucket(), source.toString());
		if (ossObject.getObjectMetadata().getContentLength() < FileCopyer.PART_SIZE_UNIT) {
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(PathDetail.getBucket(), source.toString(), PathDetail.getBucket(), target.toString());
			client.copyObject(copyObjectRequest);
		} else {
			//小文件直接拷贝,大文件需分块拷贝.
			FileCopyer.fileCopy(source.toString(), target.toString());
		}
	}

	/**
	 * oss本身不提供move操作.现在逻辑是先拷贝一个,然后删除原来的.
	 * @param ossPath
	 * @param target
	 */
	public void move(OssPath source, OssPath target) {
		if (!client.doesObjectExist(PathDetail.getBucket(), source.toString())) {
			throw new RuntimeException("source file not exist! source=" + source);
		}
		if (client.doesObjectExist(PathDetail.getBucket(), target.toString())) {
			throw new RuntimeException("target file exist! target=" + target);
		}
		
		OSSObject ossObject = client.getObject(PathDetail.getBucket(), source.toString());
		if (ossObject.getObjectMetadata().getContentLength() < FileCopyer.PART_SIZE_UNIT) {
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(PathDetail.getBucket(), source.toString(), PathDetail.getBucket(), target.toString());
			client.copyObject(copyObjectRequest);
		} else {
			FileCopyer.fileCopy(source.toString(), target.toString());
		}
		
		client.deleteObject(PathDetail.getBucket(), source.toString());
	}

	public <A extends BasicFileAttributes> A readAttributes(OssPath ossPath, Class<A> type, LinkOption[] options) {
		OSSObject ossObject = client.getObject(PathDetail.getBucket(), ossPath.toString());
		return (A) new OssFileAttributes(ossObject);
	}

	public void readAttributes(OssPath path, AccessMode... modes) throws IOException {
		try {
			client.getObject(PathDetail.getBucket(), path.toString());
		} catch (OSSException e) {
			throw new IOException(e);
		}
	}


}
