/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.novelbio.jsr203.bos;

import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;

/**
 * 从阿里云oos能获取到的一些文件基本属性
 */
public class OssFileAttributes implements BasicFileAttributes {
	/** Internal implementation of file status */
	private static final Logger logger = LoggerFactory.getLogger(OssFileAttributes.class);
	
	private static final OSSClient client = OssInitiator.getClient();
//	private OSSObject ossObject;
	private String key;
	private ObjectMetadata objectMetadata;
	/** 没有找到直接匹配的,找了一个加/后相同的 */
	private boolean isLikedSummary = false;
	
	public OssFileAttributes(String key, ObjectMetadata objectMetadata) {
		this.key = key;
		this.objectMetadata = objectMetadata;
	
		if (this.objectMetadata == null) {
			try {
				this.objectMetadata = client.getObjectMetadata(OssConfig.getBucket(), key + "/");
				if (this.objectMetadata != null) {
					this.isLikedSummary = true;
					this.key = key + "/";
				}
			} catch (OSSException e2) {
				logger.warn("no such key.key=" + key);
			}
		}
		
	}

	@Override
	public FileTime creationTime() {
		return FileTime.from(objectMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public Object fileKey() {
		return this.key;
	}

	@Override
	public boolean isDirectory() {
		return isLikedSummary ? true : (objectMetadata != null && key.endsWith("/"));
	}

	@Override
	public boolean isOther() {
		return false;
	}

	@Override
	public boolean isRegularFile() {
		return !this.key.endsWith("/");
	}

	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public FileTime lastAccessTime() {
		if (this.objectMetadata != null) {
			return FileTime.from(this.objectMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
		} else {
			return FileTime.from(this.objectMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
		}
		// bos只有一个最后修改时间
	}

	@Override
	public FileTime lastModifiedTime() {
		try {
			if (this.objectMetadata.getLastModified() != null) {
				return FileTime.from(this.objectMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
			} else {
				return FileTime.from(this.objectMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return FileTime.from(0l, TimeUnit.MILLISECONDS);
	}

	@Override
	public long size() {
		if (this.objectMetadata != null) {
			return this.objectMetadata.getContentLength();
		} else {
			return -1l;
		}
	}

	@Override
	public String toString() {
		return "HadoopFileAttributes [ossObject=" + this.key + "]";
	}

}
