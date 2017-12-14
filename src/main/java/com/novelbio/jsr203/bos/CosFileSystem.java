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

import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;

/**
 * 阿里云oss文件系统
 * 
 * @author novelbio
 *
 */
public class CosFileSystem extends FileSystem {
	private static final Logger logger = LoggerFactory.getLogger(CosFileSystem.class);
	
	private boolean readOnly;
	private volatile boolean isOpen = true;
	private String host;
	FileSystemProvider fileSystemProvider;
	CosFileAttributes cosFileAttributes;
	ObjectMetadata objectMetadata = null;
	COSClient client = CosInitiator.getClient();

	public CosFileSystem(FileSystemProvider fileSystemProvider, URI uri) {
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
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
		listObjectsRequest.setBucketName(PathDetailOs.getBucket());
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
		    	objectListing.getObjectSummaries().forEach(bos -> lsPaths.add(new CosPath(this, bos.getKey().getBytes())));
		    }
		    if (objectListing.getCommonPrefixes() != null && !objectListing.getCommonPrefixes().isEmpty()) {
		    	// 文件夹
		    	objectListing.getCommonPrefixes().forEach(prefix -> lsPaths.add(new CosPath(this, prefix.getBytes())));
		    }
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());
		return lsPaths;
	}

	@Override
	public Iterable<FileStore> getFileStores() {
		ArrayList<FileStore> list = new ArrayList<>(1);
		list.add(new CosFileStore(new CosPath(this, new byte[] { '/' })));
		return list;
	}

	private static final Set<String> supportedFileAttributeViews = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("cos")));
	
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
		return new CosPath(this, path.getBytes());
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
	 * @param cosPath
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	public Iterator<Path> iteratorOf(CosPath cosPath, java.nio.file.DirectoryStream.Filter<? super Path> filter) throws Exception {
		List<Path> lsCosPath = new ArrayList<>();
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
		listObjectsRequest.setBucketName(PathDetailOs.getBucket());
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		// 列出cosPath目录下的内容
		String path = cosPath.getInternalPath();
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
					URI uri = new URI("cos://" + PathDetailOs.getBucket() + "/" + name);
					lsCosPath.add(fileSystemProvider.getPath(uri));
				}
			}

			if (objectListing.getObjectSummaries() != null) {
				for (COSObjectSummary summary : objectListing.getObjectSummaries()) {
					if (summary.getKey().equals(path)) {
						// 默认返回内容中有一个cosPath,这个不需要.过滤掉.
						continue;
					}
					String name = summary.getKey();
					if (name.endsWith("/")) {
						name = name.substring(0, name.length() -1);
					}
					URI uri = new URI("cos://" + PathDetailOs.getBucket() + "/" + name);
					if (!lsCosPath.contains(fileSystemProvider.getPath(uri))) {
						lsCosPath.add(fileSystemProvider.getPath(uri));
					}
				}
			}
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());

		return lsCosPath.iterator();
	}
	
	@VisibleForTesting
	public Iterator<Path> iteratorOf(CosPath ossPath) throws Exception {
		List<Path> lsCosPath = new ArrayList<>();
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
		listObjectsRequest.setBucketName(PathDetailOs.getBucket());
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
					URI uri = new URI("cos://" + PathDetailOs.getBucket() + "/" + name);
					lsCosPath.add(fileSystemProvider.getPath(uri));
				}
			}

			if (objectListing.getObjectSummaries() != null && !objectListing.getObjectSummaries().isEmpty()) {
				for (COSObjectSummary summary : objectListing.getObjectSummaries()) {
					if (summary.getKey().equals(path)) {
						// 默认返回内容中有一个ossPath,这个不需要.过滤掉.
						continue;
					}
					URI uri = new URI("cos://" + PathDetailOs.getBucket() + "/" + summary.getKey());
					lsCosPath.add(fileSystemProvider.getPath(uri));
				}
			}
		    nextMarker = objectListing.getNextMarker();
		} while (objectListing.isTruncated());

		return lsCosPath.iterator();
	}

	public COSClient getCos() {
		return this.client;
	}
	
	/** 删除文件，文件夹会全部删除 */
	protected void deleteFile(CosPath path) {
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
			
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
			listObjectsRequest.setBucketName(PathDetailOs.getBucket());
			// 列出根目录下的所有文件和文件夹
			listObjectsRequest.setPrefix(prefix);
			listObjectsRequest.setMaxKeys(1000);
			String nextMarker = null;
			ObjectListing objectListing = null;
			List<DeleteObjectsRequest.KeyVersion> lsKeys = new ArrayList<>();
			do {
				listObjectsRequest.setMarker(nextMarker);
			    objectListing = client.listObjects(listObjectsRequest);
			    if (objectListing.getObjectSummaries() != null && !objectListing.getObjectSummaries().isEmpty()) {
			    	objectListing.getObjectSummaries().forEach(bos -> lsKeys.add(new DeleteObjectsRequest.KeyVersion(bos.getKey())));
			    }
			    nextMarker = objectListing.getNextMarker();
			} while (objectListing.isTruncated());
			
			DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(PathDetailOs.getBucket());
			if (!lsKeys.isEmpty()) {
				deleteObjectsRequest.setKeys(lsKeys);
				client.deleteObjects(deleteObjectsRequest);
			}
		} catch (CosClientException e) {
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

		client.putObject(PathDetailOs.getBucket(), path, new ByteArrayInputStream(new byte[]{}), null);
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


	public void checkAccess(CosPath bosPath, AccessMode[] modes) {
		// TODO Auto-generated method stub
	}

	public <V extends FileAttributeView> V getView(CosPath bosPath, Class<V> type) {
		// TODO Auto-generated method stub
		return null;
	}

	public InputStream newInputStream(CosPath path, OpenOption[] options) {
		GetObjectRequest getObjectRequest = new GetObjectRequest(PathDetailOs.getBucket(), path.getInternalPath());
		COSObject bosObject = client.getObject(getObjectRequest);
		return bosObject.getObjectContent();
	}

	public DirectoryStream<Path> newDirectoryStream(CosPath ossPath, Filter<? super Path> filter) throws IOException {
		return new CosDirectoryStream(ossPath, filter);
	}

	public void copy(CosPath source, CosPath target) {
		if (!client.doesObjectExist(PathDetailOs.getBucket(), source.getInternalPath())) {
			throw new RuntimeException("source file not exist! source=" + source);
		}
		if (client.doesObjectExist(PathDetailOs.getBucket(), target.getInternalPath())) {
			throw new RuntimeException("target file exist! target=" + target);
		}
		
		// add by fans.fan 170110
		createDirectory(((CosPath)target.getParent()).getInternalPath(), null);
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
	public void move(CosPath source, CosPath target) {
		if (!client.doesObjectExist(PathDetailOs.getBucket(), source.getInternalPath())) {
			throw new RuntimeException("source file not exist! source=" + source);
		}
		if (client.doesObjectExist(PathDetailOs.getBucket(), target.getInternalPath())) {
			throw new RuntimeException("target file exist! target=" + target);
		}
		
		// add by fans.fan 170110
		createDirectory(((CosPath)target.getParent()).getInternalPath(), null);
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

	public <A extends BasicFileAttributes> A readAttributes(CosPath ossPath, Class<A> type, LinkOption[] options) {
		if (objectMetadata == null) {
			if (client.doesObjectExist(PathDetailOs.getBucket(), ossPath.getInternalPath())) {
				objectMetadata = client.getObjectMetadata(PathDetailOs.getBucket(), ossPath.getInternalPath());
			}
		}
		if (cosFileAttributes == null) {
			cosFileAttributes = new CosFileAttributes(ossPath.getInternalPath(), objectMetadata);
		}
		return (A) cosFileAttributes;
	}

	public void readAttributes(CosPath path, AccessMode... modes) throws IOException {
		try {
			boolean isExist = client.doesObjectExist(COSConfig.getBucket(), path.getInternalPath());
			if (!isExist && !path.getInternalPath().endsWith("/")) {
				isExist = client.doesObjectExist(COSConfig.getBucket(), path.getInternalPath() + "/");
				if (!isExist) {
					logger.info("file not exist." + path.getInternalPath());
					throw new IOException();
				}
			} else if(!isExist) {
				throw new IOException();
			}
		} catch (CosClientException e) {
			throw new IOException(e);
		}
	}

	public String getHost() {
		return host;
	}

}
