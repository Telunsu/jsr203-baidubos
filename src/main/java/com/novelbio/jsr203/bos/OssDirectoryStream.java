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
import java.net.URISyntaxException;
import java.nio.file.ClosedDirectoryStreamException;
import java.nio.file.DirectoryStream;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ObjectListing;

public class OssDirectoryStream implements DirectoryStream<Path> {
    private final OssFileSystem oosFileSystem;
    //private final byte[] path;
	private final OssPath path;
    private final DirectoryStream.Filter<? super Path> filter;
    private volatile boolean isClosed;
    private volatile Iterator<Path> itr;

    OssDirectoryStream(OssPath ossPath,
                       DirectoryStream.Filter<? super java.nio.file.Path> filter)
        throws IOException
    {
        this.oosFileSystem = ossPath.getFileSystem();
    	this.path = ossPath;
        this.filter = filter;
        
        
      
    }

    @Override
	public synchronized Iterator<Path> iterator() {
		if (isClosed)
			throw new ClosedDirectoryStreamException();
		if (itr != null)
			throw new IllegalStateException("Iterator has already been returned");

		try {
			itr = this.oosFileSystem.iteratorOf(path, filter);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		return new Iterator<Path>() {
			// private Path next;
			@Override
			public boolean hasNext() {
				if (isClosed)
					return false;
				return itr.hasNext();
			}

			@Override
			public synchronized Path next() {
				if (isClosed)
					throw new NoSuchElementException();
				return itr.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

    @Override
    public synchronized void close() throws IOException {
        isClosed = true;
    }
}