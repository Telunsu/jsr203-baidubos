package com.novelbio.jsr203.bos;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 阿里云oss的文件操作实现类.
 * 
 * @author novelbio
 *
 */
public class CosPath implements Path {

	private byte[] path;
	/** Store offsets of '/' chars */
	private volatile int[] offsets;
	private String internalPath;
	private final CosFileSystem cosFileSystem;
	private int hashcode = 0; // cached hash code (created lazily)

	CosPath(FileSystem bfs, byte[] path) {
		this(bfs, path, false);
	}

	CosPath(FileSystem bfs, byte[] path, boolean normalized) {
		assert bfs != null;

		this.cosFileSystem = (CosFileSystem) bfs;
		this.path = normalized ? path : normalize(path);
		// TODO : add charset management
		this.internalPath = new String(this.path);
	}

	private CosPath checkPath(Path path) {
		if (path == null)
			throw new NullPointerException();
		if (!(path instanceof CosPath))
			throw new ProviderMismatchException();
		return (CosPath) path;
	}

	@Override
	public int hashCode() {
		int h = hashcode;
		if (h == 0)
			hashcode = h = Arrays.hashCode(path);
		return h;
	}

	@Override
	public boolean equals(Object obj) {
		return obj != null && obj instanceof CosPath && this.cosFileSystem.equals(((CosPath) obj).cosFileSystem) && compareTo((Path) obj) == 0;
	}

	@Override
	public int compareTo(Path other) {
		final CosPath o = checkPath(other);
		return this.internalPath.compareTo(o.internalPath);
	}

	@Override
	public boolean endsWith(Path other) {
		final CosPath o = checkPath(other);
		int olast = o.path.length - 1;
		if (olast > 0 && o.path[olast] == '/')
			olast--;
		int last = this.path.length - 1;
		if (last > 0 && this.path[last] == '/')
			last--;
		if (olast == -1) // o.path.length == 0
			return last == -1;
		if ((o.isAbsolute() && (!this.isAbsolute() || olast != last)) || (last < olast))
			return false;
		for (; olast >= 0; olast--, last--) {
			if (o.path[olast] != this.path[last])
				return false;
		}
		return o.path[olast + 1] == '/' || last == -1 || this.path[last] == '/';
	}

	@Override
	public boolean endsWith(String other) {
		return endsWith(getFileSystem().getPath(other));
	}

	@Override
	public Path getFileName() {
		initOffsets();
		int count = offsets.length;
		if (count == 0)
			return null; // no elements so no name
		if (count == 1 && path[0] != '/')
			return this;
		int lastOffset = offsets[count - 1];
		int len = path.length - lastOffset;
		byte[] result = new byte[len];
		System.arraycopy(path, lastOffset, result, 0, len);
		return new CosPath(this.cosFileSystem, result);
	}

	@Override
	public CosFileSystem getFileSystem() {
		return this.cosFileSystem;
	}

	@Override
	public Path getName(int index) {
		initOffsets();
		if (index < 0 || index >= offsets.length)
			throw new IllegalArgumentException();
		int begin = offsets[index];
		int len;
		if (index == (offsets.length - 1))
			len = path.length - begin;
		else
			len = offsets[index + 1] - begin - 1;
		// construct result
		byte[] result = new byte[len];
		System.arraycopy(path, begin, result, 0, len);
		return new CosPath(this.cosFileSystem, result);
	}

	@Override
	public int getNameCount() {
		initOffsets();
		return offsets.length;
	}

	@Override
	public Path getParent() {
		initOffsets();
		int count = offsets.length;
		if (count == 0) // no elements so no parent
			return new CosPath(this.cosFileSystem, new byte[] {});
		int len = offsets[count - 1] - 1;
//		int len = offsets[count - 1];
		if (len <= 0) // parent is root only (may be null)
			return getRoot();
		byte[] result = new byte[len];
		System.arraycopy(path, 0, result, 0, len);
		return new CosPath(this.cosFileSystem, result);
	}

	@Override
	public Path getRoot() {
		if (this.isAbsolute())
			return new CosPath(this.cosFileSystem, new byte[] { path[0] });
		else
			return new CosPath(this.cosFileSystem, new byte[] {});
	}

	@Override
	public boolean isAbsolute() {
		return (this.path.length > 0 && path[0] == '/');
	}

	@Override
	public Iterator<Path> iterator() {
		return asList().iterator();
	}

	private List<Path> asList() {
		return new AbstractList<Path>() {
			@Override
			public Path get(int index) {
				return getName(index);
			}

			@Override
			public int size() {
				return getNameCount();
			}
		};
	}

	@Override
	public Path normalize() {
		byte[] resolved = getResolved();
		if (resolved == path) // no change
			return this;
		return new CosPath(this.cosFileSystem, resolved, true);
	}

	// removes redundant slashs, replace "\" to hadoop separator "/"
	// and check for invalid characters
	private byte[] normalize(byte[] path) {
		if (path.length == 0)
			return path;
		byte prevC = 0;
		for (int i = 0; i < path.length; i++) {
			byte c = path[i];
			if (c == '\\')
				return normalize(path, i);
			if (c == (byte) '/' && prevC == '/')
				return normalize(path, i - 1);
			if (c == '\u0000')
				throw new InvalidPathException(this.cosFileSystem.getString(path), "Path: nul character not allowed");
			prevC = c;
		}
		return path;
	}

	private byte[] normalize(byte[] path, int off) {
		byte[] to = new byte[path.length];
		int n = 0;
		while (n < off) {
			to[n] = path[n];
			n++;
		}
		int m = n;
		byte prevC = 0;
		while (n < path.length) {
			byte c = path[n++];
			if (c == (byte) '\\')
				c = (byte) '/';
			if (c == (byte) '/' && prevC == (byte) '/')
				continue;
			if (c == '\u0000')
				throw new InvalidPathException(this.cosFileSystem.getString(path), "Path: nul character not allowed");
			to[m++] = c;
			prevC = c;
		}
		if (m > 1 && to[m - 1] == '/')
			m--;
		return (m == to.length) ? to : Arrays.copyOf(to, m);
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>... events) throws IOException {
		return register(watcher, events, new WatchEvent.Modifier[0]);
	}

	@Override
	public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException {
		throw new UnsupportedOperationException();
	}

	private boolean equalsNameAt(CosPath other, int index) {
		int mbegin = offsets[index];
		int mlen = 0;
		if (index == (offsets.length - 1))
			mlen = path.length - mbegin;
		else
			mlen = offsets[index + 1] - mbegin - 1;
		int obegin = other.offsets[index];
		int olen = 0;
		if (index == (other.offsets.length - 1))
			olen = other.path.length - obegin;
		else
			olen = other.offsets[index + 1] - obegin - 1;
		if (mlen != olen)
			return false;
		int n = 0;
		while (n < mlen) {
			if (path[mbegin + n] != other.path[obegin + n])
				return false;
			n++;
		}
		return true;
	}

	@Override
	public Path relativize(Path other) {
		final CosPath o = checkPath(other);
		if (o.equals(this))
			return new CosPath(getFileSystem(), new byte[0], true);
		if (/* this.getFileSystem() != o.getFileSystem() || */
		this.isAbsolute() != o.isAbsolute()) {
			throw new IllegalArgumentException();
		}
		int mc = this.getNameCount();
		int oc = o.getNameCount();
		int n = Math.min(mc, oc);
		int i = 0;
		while (i < n) {
			if (!equalsNameAt(o, i))
				break;
			i++;
		}
		int dotdots = mc - i;
		int len = dotdots * 3 - 1;
		if (i < oc)
			len += (o.path.length - o.offsets[i] + 1);
		byte[] result = new byte[len];

		int pos = 0;
		while (dotdots > 0) {
			result[pos++] = (byte) '.';
			result[pos++] = (byte) '.';
			if (pos < len) // no tailing slash at the end
				result[pos++] = (byte) '/';
			dotdots--;
		}
		if (i < oc)
			System.arraycopy(o.path, o.offsets[i], result, pos, o.path.length - o.offsets[i]);
		return new CosPath(getFileSystem(), result);
	}

	@Override
	public Path resolve(Path other) {
		final CosPath o = checkPath(other);
		if (o.isAbsolute())
			return o;
		byte[] resolved = null;
		if (this.path.length == 0) {
			// this method contract explicitly specifies this behavior in the
			// case of an empty path
			return o;
		} else if (this.path[path.length - 1] == '/') {
			resolved = new byte[path.length + o.path.length];
			System.arraycopy(path, 0, resolved, 0, path.length);
			System.arraycopy(o.path, 0, resolved, path.length, o.path.length);
		} else {
			resolved = new byte[path.length + 1 + o.path.length];
			System.arraycopy(path, 0, resolved, 0, path.length);
			resolved[path.length] = '/';
			System.arraycopy(o.path, 0, resolved, path.length + 1, o.path.length);
		}
		return new CosPath(cosFileSystem, resolved);
	}

	@Override
	public Path resolve(String other) {
		return resolve(getFileSystem().getPath(other));
	}

	@Override
	public Path resolveSibling(Path other) {
		if (other == null)
			throw new NullPointerException();
		Path parent = getParent();
		return (parent == null) ? other : parent.resolve(other);
	}

	@Override
	public Path resolveSibling(String other) {
		return resolveSibling(getFileSystem().getPath(other));
	}

	@Override
	public boolean startsWith(Path other) {
		final CosPath o = checkPath(other);
		if (o.isAbsolute() != this.isAbsolute() || o.path.length > this.path.length)
			return false;
		int olast = o.path.length;
		for (int i = 0; i < olast; i++) {
			if (o.path[i] != this.path[i])
				return false;
		}
		olast--;
		return o.path.length == this.path.length || o.path[olast] == '/' || this.path[olast + 1] == '/';
	}

	@Override
	public boolean startsWith(String other) {
		return startsWith(getFileSystem().getPath(other));
	}

	@Override
	public CosPath subpath(int beginIndex, int endIndex) {
		initOffsets();
		if (beginIndex < 0 || beginIndex >= offsets.length || endIndex > offsets.length || beginIndex >= endIndex)
			throw new IllegalArgumentException();

		// starting offset and length
		int begin = offsets[beginIndex];
		int len;
		if (endIndex == offsets.length)
			len = path.length - begin;
		else
			len = offsets[endIndex] - begin - 1;
		// construct result
		byte[] result = new byte[len];
		System.arraycopy(path, begin, result, 0, len);
		return new CosPath(this.cosFileSystem, result);
	}

	@Override
	public CosPath toAbsolutePath() {
		if (isAbsolute()) {
			return this;
		} else {
			// add / before the existing path
			byte[] defaultdir = "/".getBytes(); // this.bfs.getDefaultDir().path;
			int defaultlen = defaultdir.length;
			boolean endsWith = (defaultdir[defaultlen - 1] == '/');
			byte[] t = null;
			if (endsWith)
				t = new byte[defaultlen + path.length];
			else
				t = new byte[defaultlen + 1 + path.length];
			System.arraycopy(defaultdir, 0, t, 0, defaultlen);
			if (!endsWith)
				t[defaultlen++] = '/';
			System.arraycopy(path, 0, t, defaultlen, path.length);
			return new CosPath(this.cosFileSystem, t, true); // normalized
		}
	}

	public String toAbsolutePathStr() {
		if (isAbsolute()) {
			return this.toString();
		} else {
			// add / before the existing path
			byte[] defaultdir = "/".getBytes(); // this.bfs.getDefaultDir().path;
			int defaultlen = defaultdir.length;
			boolean endsWith = (defaultdir[defaultlen - 1] == '/');
			byte[] t = null;
			if (endsWith)
				t = new byte[defaultlen + path.length];
			else
				t = new byte[defaultlen + 1 + path.length];
			System.arraycopy(defaultdir, 0, t, 0, defaultlen);
			if (!endsWith)
				t[defaultlen++] = '/';
			System.arraycopy(path, 0, t, defaultlen, path.length);
			CosPath path = new CosPath(this.cosFileSystem, t, true); // normalized
			return path.toString();
		}
	}

	@Override
	public File toFile() {
		// No, just no.
		throw new UnsupportedOperationException();
	}

	@Override
	public Path toRealPath(LinkOption... options) throws IOException {
		CosPath realPath = new CosPath(this.cosFileSystem, getResolvedPath()).toAbsolutePath();
		return realPath;
	}

	@Override
	public URI toUri() {
//		int len = toAbsolutePath().path.length;
//		if (len > 0) {
//			len = len -1;
//		}
//		byte[] resPath = new byte[len];
//		if (toAbsolutePath().path.length > 0 && toAbsolutePath().path[0] == '/') {
//			System.arraycopy(path, 1, resPath, 0, len);
//		}
		try {
			if (cosFileSystem.getHost() != null && !"".equals(cosFileSystem.getHost())) {
				return new URI(CosFileSystemProvider.SCHEME + "://" + cosFileSystem.getHost() + new String(toAbsolutePath().path));
			} else {
				return new URI(CosFileSystemProvider.SCHEME + "://" + PathDetailOs.getBucket() + new String(toAbsolutePath().path));
			}
		} catch (URISyntaxException e) {
			throw new AssertionError(e);
		}
	}

	void createDirectory(FileAttribute<?>... attrs) throws IOException {
		this.cosFileSystem.createDirectory(this.getInternalPath(), attrs);
	}

	@Override
	public String toString() {
//		if (isAbsolute()) {
////			return new String(CosFileSystemProvider.SCHEME + new String(getResolvedPath()));
//			return new String(getResolvedPath());
//		} else {
//			return new String(this.path);
//		}
		
		if (isAbsolute()) {
			return toUri().toString();
		} else {
			return new String(this.path);
		}
	}

	DirectoryStream<Path> newDirectoryStream(Filter<? super Path> filter) throws IOException {
		return this.cosFileSystem.newDirectoryStream(this, filter);
	}

	void delete() throws IOException {
		this.cosFileSystem.deleteFile(this);
	}

	void deleteIfExists() throws IOException {
		this.cosFileSystem.deleteFile(this);
	}

	void move(CosPath target, CopyOption... options) throws IOException {
		this.cosFileSystem.move(this, target);
	}
	
	// the result path does not contain ./ and .. components
	private volatile byte[] resolved = null;

	byte[] getResolvedPath() {
		byte[] r = resolved;
		if (r == null) {
			if (isAbsolute())
				r = getResolved();
			else
				r = toAbsolutePath().getResolvedPath();
			// if (r[0] == '/')
			// r = Arrays.copyOfRange(r, 1, r.length);
			resolved = r;
		}
		return resolved;
	}

	// Remove DotSlash(./) and resolve DotDot (..) components
	private byte[] getResolved() {
		if (path.length == 0)
			return path;
		for (int i = 0; i < path.length; i++) {
			byte c = path[i];
			if (c == (byte) '.')
				return resolve0();
		}
		return path;
	}

	// TBD: performance, avoid initOffsets
	private byte[] resolve0() {
		byte[] to = new byte[path.length];
		int nc = getNameCount();
		int[] lastM = new int[nc];
		int lastMOff = -1;
		int m = 0;
		for (int i = 0; i < nc; i++) {
			int n = offsets[i];
			int len = (i == offsets.length - 1) ? (path.length - n) : (offsets[i + 1] - n - 1);
			if (len == 1 && path[n] == (byte) '.') {
				if (m == 0 && path[0] == '/') // absolute path
					to[m++] = '/';
				continue;
			}
			if (len == 2 && path[n] == '.' && path[n + 1] == '.') {
				if (lastMOff >= 0) {
					m = lastM[lastMOff--]; // retreat
					continue;
				}
				if (path[0] == '/') { // "/../xyz" skip
					if (m == 0)
						to[m++] = '/';
				} else { // "../xyz" -> "../xyz"
					if (m != 0 && to[m - 1] != '/')
						to[m++] = '/';
					while (len-- > 0)
						to[m++] = path[n++];
				}
				continue;
			}
			if (m == 0 && path[0] == '/' || // absolute path
					m != 0 && to[m - 1] != '/') { // not the first name
				to[m++] = '/';
			}
			lastM[++lastMOff] = m;
			while (len-- > 0)
				to[m++] = path[n++];
		}
		if (m > 1 && to[m - 1] == '/')
			m--;
		return (m == to.length) ? to : Arrays.copyOf(to, m);
	}

	boolean exists() {
		// Root case
		if ("/".equals(internalPath))
			return true;
		try {
//			return bfs.exists(getRawResolvedPath());
		} catch (Exception x) {
		}
		return false;
	}

	public Path getRawResolvedPath() {
//		return new BosPath(bfs, "bos://" + new String(getResolvedPath()));
		return null;
	}
	
	// create offset list if not already created
	private void initOffsets() {
		if (offsets == null) {
			int count, index;
			// count names
			count = 0;
			index = 0;
			while (index < path.length) {
				byte c = path[index++];
				if (c != '/') {
					count++;
					while (index < path.length && path[index] != '/')
						index++;
				}
			}
			// populate offsets
			int[] result = new int[count];
			count = 0;
			index = 0;
			while (index < path.length) {
				byte c = path[index];
				if (c == '/') {
					index++;
				} else {
					result[count++] = index++;
					while (index < path.length && path[index] != '/')
						index++;
				}
			}
			synchronized (this) {
				if (offsets == null)
					offsets = result;
			}
		}
	}

	void copy(CosPath target, CopyOption... options) throws IOException {
		this.cosFileSystem.copy(this, target);
	}


	public InputStream newInputStream(Path path, OpenOption... options) {
		return this.cosFileSystem.newInputStream((CosPath) path, options);
	}

	public <A extends BasicFileAttributes> A readAttributes(Class<A> type, LinkOption[] options) {
		return this.cosFileSystem.readAttributes(this, type, options);
	}

	public void checkAccess(AccessMode... modes) throws IOException {
		this.cosFileSystem.readAttributes(this, modes);
	}
	
	/**
	 * 对应阿里云cos的key的路径格式.也就是不带前缀了.
	 * 
	 * @return
	 */
	public String getInternalPath() {
		if (internalPath != null && internalPath.startsWith("/")) {
			return internalPath.substring(1);
		} else {
			return internalPath;
		}
		
	}
}
