package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OssFileSystemProvider extends FileSystemProvider {

	private static final Logger logger = LoggerFactory.getLogger(OssFileSystemProvider.class);
	
	public static final String SCHEME = "oss";
	
	  // Checks that the given file is a HadoopPath
	static final OssPath toOssPath(Path path) {
		if (path == null) {
			throw new NullPointerException();
		}
		if (!(path instanceof OssPath)) {
			throw new ProviderMismatchException();
		}
		return (OssPath)path;
	}
	
	@Override
	public String getScheme() {
		return SCHEME;
	}

	@Override
	public FileSystem getFileSystem(URI uri) {
		try {
			return newFileSystem(uri, Collections.<String, Object> emptyMap());
		} catch (IOException e) {
			logger.error("Problem instantiating HadoopFileSystem: ", e);
			throw new FileSystemNotFoundException(e.getMessage());
		}
	}
	
	@Override
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		return new OssFileSystem(this, uri);
	}

	@Override
	public Path getPath(URI uri) {
		return getFileSystem(uri).getPath(uri.getPath());
	}

	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		return new ObjectSeekableByteStream(PathDetailOs.getBucket(), ((OssPath)path).getInternalPath());
	}
	
	/**
	 * 重写newInputStream方法.如果不重写,默认实现会调用newByteChannel方法.实际测试该方法每次read都去联一次远程地址,超级慢.
	 * @see java.nio.file.spi.FileSystemProvider#newInputStream(java.nio.file.Path, java.nio.file.OpenOption[])
	 */
	@Override
	public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
		return toOssPath(path).newInputStream(path, options);
	}
	
	/* 获取文件夹信息,如里面的文件
	 * @see java.nio.file.spi.FileSystemProvider#newDirectoryStream(java.nio.file.Path, java.nio.file.DirectoryStream.Filter)
	 */
	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
		return toOssPath(dir).newDirectoryStream(filter);
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		toOssPath(dir).createDirectory(attrs);
	}

	@Override
	public void delete(Path path) throws IOException {
		toOssPath(path).delete();
	}
	
	@Override
	public boolean deleteIfExists(Path path) throws IOException {
		delete(path);
		return true;
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		toOssPath(source).copy(toOssPath(target), options);
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		toOssPath(source).move(toOssPath(target), options);
	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		return toOssPath(path).compareTo(toOssPath(path2)) == 0;
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		return false;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		throw new RuntimeException("no supported!");
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
//		toBosPath(path).getFileSystem().checkAccess(toBosPath(path), modes);
//		throw new UnsupportedOperationException("method no supported! method=checkAccess");
		toOssPath(path).checkAccess(modes);
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
//		return toBosPath(path).getFileSystem().getView(toBosPath(path), type);
//		throw new UnsupportedOperationException("method no supported! method=getFileAttributeView");
		return (V) new OssFileAttrbuteView(toOssPath(path));
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
//		if (type == BasicFileAttributes.class || type == BosFileAttributes.class)
//			return (A) toBosPath(path).getAttributes();
//		
//		if (type == PosixFileAttributes.class)
//			return (A) toBosPath(path).getPosixAttributes();
		
//		throw new UnsupportedOperationException("method no supported! method=readAttributes");
		// move 方法会调用
		return (A) toOssPath(path).readAttributes(type, options);
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		// TODO 
//		toBosPath(path).getFileSystem().readAttributes(toBosPath(path), attributes, options);
		throw new UnsupportedOperationException("method no supported! method=readAttributes");
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		// TODO Auto-generated method stub
//		toBosPath(path).getFileSystem().setAttribute(toBosPath(path), attribute, value, options);
		throw new UnsupportedOperationException("method no supported! method=setAttribute");
	}

}
