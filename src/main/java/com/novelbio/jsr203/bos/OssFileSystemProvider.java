package com.novelbio.jsr203.bos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;


public class OssFileSystemProvider extends FileSystemProvider {

	private static final Logger logger = LoggerFactory.getLogger(OssFileSystemProvider.class);
	
	public static final String SCHEME = "bos";
	
	  // Checks that the given file is a HadoopPath
	static final OssPath toBosPath(Path path) {
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
	public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
		return new OssFileSystem(this);
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
	public Path getPath(URI uri) {
		return getFileSystem(uri).getPath(uri.getPath());
	}

    /**
     * Opens a file, returning an input stream to read from the file. This
     * method works in exactly the manner specified by the {@link
     * Files#newInputStream} method.
     *
     * <p> The default implementation of this method opens a channel to the file
     * as if by invoking the {@link #newByteChannel} method and constructs a
     * stream that reads bytes from the channel. This method should be overridden
     * where appropriate.
     *
     * @param   path
     *          the path to the file to open
     * @param   options
     *          options specifying how the file is opened
     *
     * @return  a new input stream
     *
     * @throws  IllegalArgumentException
     *          if an invalid combination of options is specified
     * @throws  UnsupportedOperationException
     *          if an unsupported option is specified
     * @throws  IOException
     *          if an I/O error occurs
     * @throws  SecurityException
     *          In the case of the default provider, and a security manager is
     *          installed, the {@link SecurityManager#checkRead(String) checkRead}
     *          method is invoked to check read access to the file.
     */
//    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
//        if (options.length > 0) {
//            for (OpenOption opt: options) {
//                // All OpenOption values except for APPEND and WRITE are allowed
//                if (opt == StandardOpenOption.APPEND ||
//                    opt == StandardOpenOption.WRITE)
//                    throw new UnsupportedOperationException("'" + opt + "' not allowed");
//            }
//        }
//        //TODO
//        return null;
//    }
    
	@Override
	public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
		return new ObjectSeekableByteStream(PathDetail.getBucket(), path.toString());
	}
	
	/**
	 * 重写newInputStream方法.如果不重写,默认实现会调用newByteChannel方法.实际测试该方法每次read都去联一次远程地址,超级慢.
	 * @see java.nio.file.spi.FileSystemProvider#newInputStream(java.nio.file.Path, java.nio.file.OpenOption[])
	 */
	@Override
	public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
		return toBosPath(path).newInputStream(path, options);
	}
	
//	@Override
//	public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
//		if (options ==null || !Sets.newHashSet(options).contains(StandardOpenOption.CREATE)) {
//			throw new RuntimeException("only support option:CREATE!");
//		} 
//		
//		return toBosPath(path).newOutputStream(path);
//	}

	@Override
	public DirectoryStream<Path> newDirectoryStream(Path dir, Filter<? super Path> filter) throws IOException {
		return toBosPath(dir).newDirectoryStream(filter);
	}

	@Override
	public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
		toBosPath(dir).createDirectory(attrs);
	}

	@Override
	public void delete(Path path) throws IOException {
		toBosPath(path).delete();
	}

	@Override
	public void copy(Path source, Path target, CopyOption... options) throws IOException {
		toBosPath(source).copy(toBosPath(target), options);
	}

	@Override
	public void move(Path source, Path target, CopyOption... options) throws IOException {
		toBosPath(source).move(toBosPath(target), options);
	}

	@Override
	public boolean isSameFile(Path path, Path path2) throws IOException {
		return toBosPath(path).compareTo(toBosPath(path2)) == 0;
	}

	@Override
	public boolean isHidden(Path path) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileStore getFileStore(Path path) throws IOException {
		return toBosPath(path).getFileStore();
	}

	@Override
	public void checkAccess(Path path, AccessMode... modes) throws IOException {
		toBosPath(path).getFileSystem().checkAccess(toBosPath(path), modes);
	}

	@Override
	public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
		return toBosPath(path).getFileSystem().getView(toBosPath(path), type);
	}

	@Override
	public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
//		if (type == BasicFileAttributes.class || type == BosFileAttributes.class)
//			return (A) toBosPath(path).getAttributes();
//		
//		if (type == PosixFileAttributes.class)
//			return (A) toBosPath(path).getPosixAttributes();
		
		throw new UnsupportedOperationException("readAttributes:" + type.getName());
	}

	@Override
	public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
		// TODO 
//		toBosPath(path).getFileSystem().readAttributes(toBosPath(path), attributes, options);
		return null;
	}

	@Override
	public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
		// TODO Auto-generated method stub
//		toBosPath(path).getFileSystem().setAttribute(toBosPath(path), attribute, value, options);
	}

}
