package com.mycompany.test;

import com.azure.core.util.FluxUtil;
import com.azure.storage.common.ParallelTransferOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileAsyncClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakePathClient;
import com.azure.storage.file.datalake.DataLakePathClientBuilder;
import com.azure.storage.file.datalake.models.PathInfo;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Random;

public class UploadFileTest {
  private DataLakePathClientBuilder pathClientBuilder;

  public UploadFileTest(String accountName, String accessToken) {
    String endpoint = String.format(Locale.ROOT,
                          "https://%s.dfs.core.windows.net", accountName);
    StorageSharedKeyCredential credential
        = new StorageSharedKeyCredential(accountName, accessToken);
    pathClientBuilder = new DataLakePathClientBuilder()
                            .endpoint(endpoint).credential(credential);
  }

  public void uploadFile(String localPath, String remotePath)
      throws IOException {
    DataLakeFileAsyncClient fileAsyncClient = getFileAsyncClient(remotePath);
    MonoSubscriber subscriber = new MonoSubscriber<Void>();
    boolean success = false;
    try {
      Mono<Void> mono = fileAsyncClient.uploadFromFile(localPath, true);
      mono.subscribe(subscriber);
      success = subscriber.waitForCompletion();
    } catch(Exception e) {
      throw new IOException(e);
    }
    Throwable t = subscriber.getThrowable();
    if (t != null) { // Some exception occurred during data transfer
      throw new IOException(t);
    }
    if (!success) {
      throw new IOException("Upload timed out");
    }
  }

  public void uploadInputStream(String localPath, String remotePath)
      throws IOException {
    DataLakeFileAsyncClient fileAsyncClient = getFileAsyncClient(remotePath);
    MonoSubscriber subscriber = new MonoSubscriber<PathInfo>();
    boolean success = false;
    try {
      Flux<ByteBuffer> publisher
          = FluxUtil.toFluxByteBuffer(new FileInputStream(localPath));
      ParallelTransferOptions transferOptions = new ParallelTransferOptions();
      Mono<PathInfo> mono = fileAsyncClient.upload(publisher, transferOptions,
                                                   true);
      mono.subscribe(subscriber);
      success = subscriber.waitForCompletion();
    } catch(Exception e) {
      throw new IOException(e);
    }
    Throwable t = subscriber.getThrowable();
    if (t != null) { // Some exception occurred during data transfer
      throw new IOException(t);
    }
    if (!success) {
      throw new IOException("Upload timed out");
    }
  }

  public boolean deleteFile(String remotePath) throws IOException {
    DataLakeFileClient fileClient = getFileClient(remotePath);
    try {
      fileClient.delete();
    } catch(Exception e) {
      throw new IOException(e);
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 5) {
      UploadFileTest test = new UploadFileTest(args[0], args[1]);
      try {
        long fileSize = Long.parseLong(args[3]);
        System.out.println("Creating local file");
        createLocalFile(args[2], fileSize);
        System.out.println("Uploading using uploadFromFile()");
        test.uploadFile(args[2], args[4]);
        System.out.println("Uploading using upload()");
        test.uploadInputStream(args[2], args[4]);
        System.out.println("Test successful");
      } finally {
        System.out.println("Deleting local file");
        new File(args[2]).delete();
        System.out.println("Deleting remote file");
        test.deleteFile(args[4]);
      }
    } else {
      System.err.println("Usage: java -jar <jarFile> <accountName>"
          + " <accessToken> <localPathName> <localFileSize> <remotePathName>");
      System.exit(1);
    }
  }

  private static void createLocalFile(String pathName, long fileSize)
      throws IOException {
    int BUF_SIZE = 10*1024;
    FileOutputStream output = new FileOutputStream(pathName);
    Random random = new Random();
    byte[] record = new byte[BUF_SIZE];
    try {
      for(; fileSize > 0; fileSize -= BUF_SIZE) {
        random.nextBytes(record);
        output.write(record);
      }
    } finally {
      output.close();
    }
  }

  private String[] splitPath(String remotePath) throws IOException {
    if (remotePath.startsWith("/")) {
      // Remove any leading "/"
      remotePath = remotePath.substring(1);
    }
    int delimLoc = remotePath.indexOf('/');
    String bucket;
    String object;
    if (delimLoc != -1) {
      bucket = remotePath.substring(0, delimLoc);
      object = remotePath.substring(delimLoc + 1);
    } else {
      throw new IOException("Invalid remote path");
    }
    String[] pathComponents = {bucket, object};
    return pathComponents;
  }

  private DataLakeFileAsyncClient getFileAsyncClient(String remotePath)
      throws IOException {
    String[] pathComponents = splitPath(remotePath);
    DataLakeFileAsyncClient fileAsyncClient = null;
    try {
      fileAsyncClient = pathClientBuilder
                        .fileSystemName(pathComponents[0])
                        .pathName(pathComponents[1])
                        .buildFileAsyncClient();
    } catch(Exception e) {
      throw new IOException(e);
    }
    return fileAsyncClient;
  }

  private DataLakeFileClient getFileClient(String remotePath)
      throws IOException {
    String[] pathComponents = splitPath(remotePath);
    DataLakeFileClient fileClient = null;
    try {
      fileClient = pathClientBuilder
                   .fileSystemName(pathComponents[0])
                   .pathName(pathComponents[1])
                   .buildFileClient();
    } catch(Exception e) {
      throw new IOException(e);
    }
    return fileClient;
  }

}
