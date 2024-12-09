/*
 * Copyright KhulnaSoft, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.khulnasoft.oss.ksbulk.io;

import com.khulnasoft.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public final class CompressedIOUtils {

  private static final int BUFFER_SIZE = 8192 * 2;

  public static final String NONE_COMPRESSION = "none";
  public static final String XZ_COMPRESSION = "xz";
  public static final String GZIP_COMPRESSION = "gzip";
  public static final String ZSTD_COMPRESSION = "zstd";
  public static final String BZIP2_COMPRESSION = "bzip2";
  public static final String SNAPPY_COMPRESSION = "snappy";
  public static final String LZ4_COMPRESSION = "lz4";
  public static final String LZMA_COMPRESSION = "lzma";
  public static final String BROTLI_COMPRESSION = "brotli";
  public static final String DEFLATE_COMPRESSION = "deflate";
  public static final String DEFLATE64_COMPRESSION = "deflate64";
  public static final String Z_COMPRESSION = "z";

  // we may have different supported compressions for input & output
  private static final ImmutableMap<String, String> OUTPUT_COMPRESSORS =
      ImmutableMap.<String, String>builder()
          .put(XZ_COMPRESSION, CompressorStreamFactory.XZ)
          .put(GZIP_COMPRESSION, CompressorStreamFactory.GZIP)
          .put(ZSTD_COMPRESSION, CompressorStreamFactory.ZSTANDARD)
          .put(BZIP2_COMPRESSION, CompressorStreamFactory.BZIP2)
          .put(SNAPPY_COMPRESSION, CompressorStreamFactory.SNAPPY_FRAMED)
          .put(LZ4_COMPRESSION, CompressorStreamFactory.LZ4_FRAMED)
          .put(LZMA_COMPRESSION, CompressorStreamFactory.LZMA)
          .put(DEFLATE_COMPRESSION, CompressorStreamFactory.DEFLATE)
          .build();

  private static final ImmutableMap<String, String> INPUT_COMPRESSORS =
      ImmutableMap.<String, String>builder()
          .put(XZ_COMPRESSION, CompressorStreamFactory.XZ)
          .put(GZIP_COMPRESSION, CompressorStreamFactory.GZIP)
          .put(ZSTD_COMPRESSION, CompressorStreamFactory.ZSTANDARD)
          .put(BZIP2_COMPRESSION, CompressorStreamFactory.BZIP2)
          .put(SNAPPY_COMPRESSION, CompressorStreamFactory.SNAPPY_FRAMED)
          .put(LZ4_COMPRESSION, CompressorStreamFactory.LZ4_FRAMED)
          .put(LZMA_COMPRESSION, CompressorStreamFactory.LZMA)
          .put(BROTLI_COMPRESSION, CompressorStreamFactory.BROTLI)
          .put(DEFLATE_COMPRESSION, CompressorStreamFactory.DEFLATE)
          .put(DEFLATE64_COMPRESSION, CompressorStreamFactory.DEFLATE64)
          .put(Z_COMPRESSION, CompressorStreamFactory.Z)
          .build();

  private static final ImmutableMap<String, String> COMPRESSION_EXTENSIONS =
      ImmutableMap.<String, String>builder()
          .put(XZ_COMPRESSION, ".xz")
          .put(GZIP_COMPRESSION, ".gz")
          .put(ZSTD_COMPRESSION, ".zstd")
          .put(BZIP2_COMPRESSION, ".bz2")
          .put(SNAPPY_COMPRESSION, ".snappy")
          .put(LZ4_COMPRESSION, ".lz4")
          .put(LZMA_COMPRESSION, ".lzma")
          .put(DEFLATE_COMPRESSION, ".deflate")
          .build();

  public static BufferedReader newBufferedReader(
      final URL url, final Charset charset, final String compression) throws IOException {
    final BufferedReader reader;
    if (compression == null || isNoneCompression(compression)) {
      reader = IOUtils.newBufferedReader(url, charset);
    } else {
      String compressor = INPUT_COMPRESSORS.get(compression.toLowerCase());
      if (compressor == null) {
        throw new IOException("Unsupported compression format: " + compression);
      }
      InputStream in = IOUtils.newBufferedInputStream(url);
      try {
        CompressorInputStream cin =
            new CompressorStreamFactory().createCompressorInputStream(compressor, in);
        reader = new BufferedReader(new InputStreamReader(cin, charset), BUFFER_SIZE);
      } catch (CompressorException ex) {
        throw new IOException("Can't instantiate class for compression: " + compression, ex);
      }
    }
    return reader;
  }

  public static BufferedWriter newBufferedWriter(
      final URL url, final Charset charset, final String compression) throws IOException {
    final BufferedWriter writer;
    if (compression == null || compression.equalsIgnoreCase(NONE_COMPRESSION))
      writer = IOUtils.newBufferedWriter(url, charset);
    else {
      String compressor = OUTPUT_COMPRESSORS.get(compression.toLowerCase());
      if (compressor == null) {
        throw new IOException("Unsupported compression format: " + compression);
      }
      OutputStream os = IOUtils.newBufferedOutputStream(url);
      try {
        CompressorOutputStream cos =
            new CompressorStreamFactory().createCompressorOutputStream(compressor, os);
        writer = new BufferedWriter(new OutputStreamWriter(cos, charset), BUFFER_SIZE);
      } catch (CompressorException ex) {
        throw new IOException("Can't instantiate class for compression: " + compression, ex);
      }
    }
    return writer;
  }

  public static String getCompressionSuffix(final String compression) {
    return COMPRESSION_EXTENSIONS.getOrDefault(compression, "");
  }

  public static List<String> getSupportedCompressions(boolean isRead) {
    List<String> lst = new ArrayList<>();
    lst.add(NONE_COMPRESSION);
    lst.addAll((isRead ? INPUT_COMPRESSORS : OUTPUT_COMPRESSORS).keySet());
    return lst;
  }

  public static Boolean isSupportedCompression(final String compression, boolean isRead) {
    if (compression == null) {
      return false;
    }
    return (isRead ? INPUT_COMPRESSORS : OUTPUT_COMPRESSORS).containsKey(compression)
        || compression.equalsIgnoreCase(NONE_COMPRESSION);
  }

  public static boolean isNoneCompression(final String compression) {
    return NONE_COMPRESSION.equalsIgnoreCase(compression);
  }
}
