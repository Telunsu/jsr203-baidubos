package com.novelbio.jsr203.bos;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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

import org.assertj.core.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.CopyObjectRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;

/**
 * 阿里云oss文件系统
 * 
 * @author novelbio
 *
 */
public class OssFileSystem extends FileSystem {
	private static final Logger logger = LoggerFactory.getLogger(OssFileSystem.class);
	
	private boolean readOnly;
	private volatile boolean isOpen = true;
	private String host;
	FileSystemProvider fileSystemProvider;
	OssFileAttributes ossFileAttributes;
	ObjectMetadata objectMetadata = null;
	OSSClient client = OssInitiator.getClient();

	public OssFileSystem(FileSystemProvider fileSystemProvider, URI uri) {
		this.fileSystemProvider = fileSystemProvider;
		this.host = uri.getHost();
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
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(PathDetailOs.getBucket());
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		// 列出根目录下的所有文件和文件夹
		listObjectsRequest.setPrefix("");
		listObjectsRequest.setMaxKeys(1000);
		String nextMarker = null;
		ObjectListing objectListing = null;
		List<Path> lsPaths = new ArrayList<>();
		do {
			listObjectsRequest.setMarker(nextMarker);
		    objectListing = client.listObjects(listObjectsRequest);
		    if (objectListing.getObjectSummaries() != null && !objectListing.getObjectSummaries().isEmpty()) {
		    	// 文件
		    	objectListing.getObjectSummaries().forEach(bos -> lsPaths.add(new OssPath(this, bos.getKey().getBytes())));
		    }
		    if (objectListing.getCommonPrefixes() != null && !objectListing.getCommonPrefixes().isEmpty()) {
		    	// 文件夹
		    	objectListing.getCommonPrefixes().forEach(prefix -> lsPaths.add(new OssPath(this, prefix.getBytes())));
		    }
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());
		return lsPaths;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		ArrayList<FileStore> list = new ArrayList<>(1);
		list.add(new OssFileStore(new OssPath(this, new byte[] { '/' })));
		return list;
	}

	private static final Set<String> supportedFileAttributeViews = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("oss")));
	
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
		
//		while (path.startsWith("/")) {
//			path = path.substring(1);
//		}
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
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(PathDetailOs.getBucket());
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		// 列出ossPath目录下的内容
		String path = ossPath.getInternalPath();
		if (!path.endsWith("/")) {
			path = path + "/";
		}
		listObjectsRequest.setPrefix(path);
		listObjectsRequest.setMaxKeys(1000);
		String nextMarker = null;
		
		ObjectListing objectListing = null;
		do {
			listObjectsRequest.setMarker(nextMarker);
		    objectListing = client.listObjects(listObjectsRequest);
			if (objectListing.getCommonPrefixes() != null && !objectListing.getCommonPrefixes().isEmpty()) {
				for (String name : objectListing.getCommonPrefixes()) {
					if (name.endsWith("/")) {
						name = name.substring(0, name.length() -1);
					}
					URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + name);
					lsOssPath.add(fileSystemProvider.getPath(uri));
				}
			}

			if (objectListing.getObjectSummaries() != null) {
				for (OSSObjectSummary summary : objectListing.getObjectSummaries()) {
					if (summary.getKey().equals(path)) {
						// 默认返回内容中有一个ossPath,这个不需要.过滤掉.
						continue;
					}
					String name = summary.getKey();
					if (name.endsWith("/")) {
						name = name.substring(0, name.length() -1);
					}
					URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + name);
					if (!lsOssPath.contains(fileSystemProvider.getPath(uri))) {
						lsOssPath.add(fileSystemProvider.getPath(uri));
					}
				}
			}
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());

		return lsOssPath.iterator();
	}
	
	@VisibleForTesting
	public Iterator<Path> iteratorOf(OssPath ossPath) throws Exception {
		List<Path> lsOssPath = new ArrayList<>();
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(PathDetailOs.getBucket());
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		// 列出ossPath目录下的内容
		String path = ossPath.getInternalPath();
		if (!path.endsWith("/")) {
			path = path + "/";
		}
		listObjectsRequest.setPrefix(path);
		listObjectsRequest.setMaxKeys(1000);
		String nextMarker = null;
		
		ObjectListing objectListing = null;
		do {
			listObjectsRequest.setMarker(nextMarker);
		    objectListing = client.listObjects(listObjectsRequest);
		    if (objectListing.getCommonPrefixes() != null && !objectListing.getCommonPrefixes().isEmpty()) {
				for (String name : objectListing.getCommonPrefixes()) {
					URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + name);
					lsOssPath.add(fileSystemProvider.getPath(uri));
				}
			}

			if (objectListing.getObjectSummaries() != null && !objectListing.getObjectSummaries().isEmpty()) {
				for (OSSObjectSummary summary : objectListing.getObjectSummaries()) {
					if (summary.getKey().equals(path)) {
						// 默认返回内容中有一个ossPath,这个不需要.过滤掉.
						continue;
					}
					URI uri = new URI("oss://" + PathDetailOs.getBucket() + "/" + summary.getKey());
					lsOssPath.add(fileSystemProvider.getPath(uri));
				}
			}
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());

		return lsOssPath.iterator();
	}

	public OSSClient getOss() {
		return this.client;
	}
	
	/** 删除文件，文件夹会全部删除 */
	protected void deleteFile(OssPath path) {
		try {
			Boolean isFileExist = null;
			if (!path.getInternalPath().endsWith("/")) {
				isFileExist = client.doesObjectExist(PathDetailOs.getBucket(), path.getInternalPath());
				if (isFileExist) {
					//是文件就直接删除
					client.deleteObject(PathDetailOs.getBucket(), path.getInternalPath());
					return;
				}
			}
			
			String prefix = path.getInternalPath();
			if (isFileExist != null) {
				//不等于null，说明查过了，但没查到
				prefix = prefix + "/";
			}
			
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest(PathDetailOs.getBucket());
			// 列出根目录下的所有文件和文件夹
			listObjectsRequest.setPrefix(prefix);
			listObjectsRequest.setMaxKeys(1000);
			String nextMarker = null;
			ObjectListing objectListing = null;
			List<String> lsKeys = new ArrayList<>();
			do {
				listObjectsRequest.setMarker(nextMarker);
			    objectListing = client.listObjects(listObjectsRequest);
			    if (objectListing.getObjectSummaries() != null && !objectListing.getObjectSummaries().isEmpty()) {
			    	objectListing.getObjectSummaries().forEach(bos -> lsKeys.add(bos.getKey()));
			    }
			    nextMarker = objectListing.getNextMarker();
			} while (objectListing.isTruncated());
			
			DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(PathDetailOs.getBucket());
			if (!lsKeys.isEmpty()) {
				deleteObjectsRequest.setKeys(lsKeys);
				client.deleteObjects(deleteObjectsRequest);
			}
		} catch (OSSException|ClientException e) {
			throw e;
		}
	}

	protected void createDirectory(String path, FileAttribute<?>[] attrs) {
		if(!path.endsWith("/")) {
			path = path + "/";
		}
		
		if (client.doesObjectExist(PathDetailOs.getBucket(), path)) {
//			logger.warn("path is existed.path=" + path);
			return;
		}
		
		client.putObject(PathDetailOs.getBucket(), path, new ByteArrayInputStream(new byte[]{}));
		// add by fans.fan 170110 递归添加文件夹
		path = path.substring(0, path.length() -1);
		int index = path.lastIndexOf("/");
		if (index > 0) {
			createDirectory(path.substring(0, index), attrs);
		}
		// end by fans.fan 
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

	public InputStream newInputStream(OssPath path, OpenOption[] options) {
		GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetailOs.getBucket(), path.getInternalPath());
		OSSObject bosObject = client.getObject(getObjectRequest);
		return bosObject.getObjectContent();
	}

	public DirectoryStream<Path> newDirectoryStream(OssPath ossPath, Filter<? super Path> filter) throws IOException {
		return new OssDirectoryStream(ossPath, filter);
	}

	public void copy(OssPath source, OssPath target) {
		if (!client.doesObjectExist(PathDetailOs.getBucket(), source.getInternalPath())) {
			throw new RuntimeException("source file not exist! source=" + source);
		}
		if (client.doesObjectExist(PathDetailOs.getBucket(), target.getInternalPath())) {
			throw new RuntimeException("target file exist! target=" + target);
		}
		
		// add by fans.fan 170110
		createDirectory(((OssPath)target.getParent()).getInternalPath(), null);
		// end by fans.fan
		
		ObjectMetadata objectMetadata = client.getObjectMetadata(PathDetailOs.getBucket(), source.getInternalPath());
		if (objectMetadata.getContentLength() < FileCopyer.PART_SIZE_UNIT) {
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(PathDetailOs.getBucket(), source.getInternalPath(), PathDetailOs.getBucket(), target.getInternalPath());
			client.copyObject(copyObjectRequest);
		} else {
			//小文件直接拷贝,大文件需分块拷贝.
			FileCopyer.fileCopy(source.getInternalPath(), target.getInternalPath());
		}
	}

	/**
	 * oss本身不提供move操作.现在逻辑是先拷贝一个,然后删除原来的.
	 * @param ossPath
	 * @param target
	 */
	public void move(OssPath source, OssPath target) {
		if (!client.doesObjectExist(PathDetailOs.getBucket(), source.getInternalPath())) {
			throw new RuntimeException("source file not exist! source=" + source);
		}
		if (client.doesObjectExist(PathDetailOs.getBucket(), target.getInternalPath())) {
			throw new RuntimeException("target file exist! target=" + target);
		}
		
		// add by fans.fan 170110
		createDirectory(((OssPath)target.getParent()).getInternalPath(), null);
		// end by fans.fan
				
		ObjectMetadata objectMetadata = client.getObjectMetadata(PathDetailOs.getBucket(), source.getInternalPath());
		if (objectMetadata.getContentLength() < FileCopyer.PART_SIZE_UNIT) {
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(PathDetailOs.getBucket(), source.getInternalPath(), PathDetailOs.getBucket(), target.getInternalPath());
			client.copyObject(copyObjectRequest);
		} else {
			FileCopyer.fileCopy(source.getInternalPath(), target.getInternalPath());
		}
		
		client.deleteObject(PathDetailOs.getBucket(), source.getInternalPath());
	}

	public <A extends BasicFileAttributes> A readAttributes(OssPath ossPath, Class<A> type, LinkOption[] options) {
		if (objectMetadata == null) {
			if (client.doesObjectExist(PathDetailOs.getBucket(), ossPath.getInternalPath())) {
				objectMetadata = client.getObjectMetadata(PathDetailOs.getBucket(), ossPath.getInternalPath());
			}
		}
		if (ossFileAttributes == null) {
			ossFileAttributes = new OssFileAttributes(ossPath.getInternalPath(), objectMetadata);
		}
		return (A) ossFileAttributes;
	}

	public void readAttributes(OssPath path, AccessMode... modes) throws IOException {
		try {
			boolean isExist = client.doesObjectExist(OssConfig.getBucket(), path.getInternalPath());
			if (!isExist && !path.getInternalPath().endsWith("/")) {
				isExist = client.doesObjectExist(OssConfig.getBucket(), path.getInternalPath() + "/");
				if (!isExist) {
					logger.info("file not exist." + path.getInternalPath());
					throw new IOException();
				}
			} else if(!isExist) {
				throw new IOException();
			}
		} catch (OSSException e) {
			throw new IOException(e);
		}
	}

	public String getHost() {
		return host;
	}

}
