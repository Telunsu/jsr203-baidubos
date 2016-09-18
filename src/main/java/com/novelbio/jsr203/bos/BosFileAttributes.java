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

import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.ObjectMetadata;

/**
 * 从百度bos能获取到的一些文件基本属性
 */
public class BosFileAttributes implements BasicFileAttributes {
	/** Internal implementation of file status */
	private final BosObjectSummary bosObjSummary;
	private final ObjectMetadata objMetadata;

	public BosFileAttributes(final BosObjectSummary bosObjSummary, final ObjectMetadata objMetadata) {
		this.bosObjSummary = bosObjSummary;
		this.objMetadata = objMetadata;
	}

	@Override
	public FileTime creationTime() {
		return FileTime.from(this.objMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public Object fileKey() {
		return this.bosObjSummary.getKey();
	}

	@Override
	public boolean isDirectory() {
		return this.bosObjSummary.getKey().endsWith("/");
	}

	@Override
	public boolean isOther() {
		return false;
	}

	@Override
	public boolean isRegularFile() {
		return !this.bosObjSummary.getKey().endsWith("/");
	}

	@Override
	public boolean isSymbolicLink() {
		return false;
	}

	@Override
	public FileTime lastAccessTime() {
		// bos只有一个最后修改时间
		return FileTime.from(this.objMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public FileTime lastModifiedTime() {
		return FileTime.from(this.objMetadata.getLastModified().getTime(), TimeUnit.MILLISECONDS);
	}

	@Override
	public long size() {
		return this.bosObjSummary.getSize();
	}

	@Override
	public String toString() {
		return "HadoopFileAttributes [objMetadata=" + objMetadata + ", bosObjSummary=" + bosObjSummary + "]";
	}

}
