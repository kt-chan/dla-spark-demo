package com.alibabacloud.cwchan

import java.io.File

object Utility {

  var tmpJKSFile:File = null;

  def getJKSFile(): String = {
    val classPathResource = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka.client.truststore.jks");
    //val lines: Iterator[String] = scala.io.Source.fromInputStream( classPathResource ).getLines
    if (tmpJKSFile == null) {
      tmpJKSFile = File.createTempFile("kafka.client.truststore-", ".jks")
    }
    try {
      java.nio.file.Files.copy(
        classPathResource, tmpJKSFile.toPath(),
        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    } finally {
      classPathResource.close();
    }

    return tmpJKSFile.getAbsolutePath();
  }
}