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

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;

/**
 * 从百度bos能获取到的一些文件基本属性
 */
public class OssFileAttributes implements BasicFileAttributes {
	/** Internal implementation of file status */
	private final OSSObject ossObject;
	private static final OSSClient client = OssInitiator.getClient();
	private OSSObjectSummary summary;
	
	public OssFileAttributes(OSSObject ossObject) {
		this.ossObject = ossObject;
		
		String path = ossObject.getKey();
		OSSObjectSummary likedSummary = null;
		ObjectListing objectListing = client.listObjects(OssConfig.getBucket(), path);
		for (OSSObjectSummary summary : objectListing.getObjectSummaries()) {
			if (summary.getKey().equals(path) || summary.getKey().equals(path + "/")) {
				this.summary = summary;
				break;
			} else if(summary.getKey().startsWith(path)) {
				likedSummary = summary;
			}
		}
		
		if (summary == null && likedSummary != null) {
			summary = new OSSObjectSummary();
			summary.setKey(path);
			summary.setLastModified(likedSummary.getLastModified());
		}
		
	}

	@Override
	public FileTime creationTime() {
		return FileTime.from(this.ossObject.getObjectMetadata().getLastModified().getTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public Object fileKey() {
		return this.ossObject.getKey();
	}

	@Override
	public boolean isDirectory() {
		return summary != null && summary.getSize() == 0;
	}

	@Override
	public boolean isOther() {
		return false;
	}

	@Override
	public boolean isRegularFile() {
		return !this.ossObject.getKey().endsWith("/");
	}

	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public FileTime lastAccessTime() {
		// bos只有一个最后修改时间
		return FileTime.from(this.ossObject.getObjectMetadata().getLastModified().getTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public FileTime lastModifiedTime() {
		try {
			if (this.ossObject.getObjectMetadata().getLastModified() != null) {
				return FileTime.from(this.ossObject.getObjectMetadata().getLastModified().getTime(), TimeUnit.MILLISECONDS);
			} else {
				return FileTime.from(this.summary.getLastModified().getTime(), TimeUnit.MILLISECONDS);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return FileTime.from(0l, TimeUnit.MILLISECONDS);
	}

	@Override
	public long size() {
		if (summary != null) {
			return this.summary.getSize();
		} else {
			return this.ossObject.getObjectMetadata().getContentLength();
		}
	}

	@Override
	public String toString() {
		return "HadoopFileAttributes [ossObject=" + this.ossObject + "]";
	}

}
