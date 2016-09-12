package com.novelbio.jsr203.bos;

import java.nio.file.Path;

public class FileStatus {
	
	private Path path;
	private long length;
	private boolean dir;
	// private short block_replication;
	// private long blocksize;
	private long modification_time;
	private long access_time;
	// private FsPermission permission;
	private String owner;
	private String group;
	private Path symlink;
	
	public Path getPath() {
		return path;
	}
	public void setPath(Path path) {
		this.path = path;
	}
	public long getLength() {
		return length;
	}
	public void setLength(long length) {
		this.length = length;
	}
	public boolean isDir() {
		return dir;
	}
	public void setDir(boolean dir) {
		this.dir = dir;
	}
	public long getModification_time() {
		return modification_time;
	}
	public void setModification_time(long modification_time) {
		this.modification_time = modification_time;
	}
	public long getAccess_time() {
		return access_time;
	}
	public void setAccess_time(long access_time) {
		this.access_time = access_time;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public Path getSymlink() {
		return symlink;
	}
	public void setSymlink(Path symlink) {
		this.symlink = symlink;
	}
	
}
