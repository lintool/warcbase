package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;

import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.ByteCountingPushBackInputStream;
import org.jwat.common.RandomAccessFileInputStream;
import org.jwat.common.UriProfile;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipReader;
import org.jwat.tools.core.FileIdent;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.warcbase.data.Util;

public class IngestFiles {
  //protected RandomAccessFile raf = null;
  
  public static long parse(File file) {
    UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;
    boolean bBlockDigestEnabled = true;
    boolean bPayloadDigestEnabled = true;
    int recordHeaderMaxSize = 8192;
    int payloadHeaderMaxSize = 32768;
    
    RandomAccessFile raf = null;
    RandomAccessFileInputStream rafin = null;
    ByteCountingPushBackInputStream pbin = null;
    GzipReader gzipReader = null;
    ArcReader arcReader = null;
    WarcReader warcReader = null;
    WarcReader warcReader2 = null;
    GzipEntry gzipEntry = null;
    ArcRecordBase arcRecord = null;
    WarcRecord warcRecord = null;
    byte[] buffer = new byte[ 8192 ];
    String uri = null;
    String key = null;
    try {
      raf = new RandomAccessFile( file, "r" );
      rafin = new RandomAccessFileInputStream( raf );
      pbin = new ByteCountingPushBackInputStream( new BufferedInputStream( rafin, 8192 ), 32 );
      if ( GzipReader.isGzipped( pbin ) ) {
        gzipReader = new GzipReader( pbin );
        ByteCountingPushBackInputStream in;
        ByteCountingPushBackInputStream in2;
        int gzipEntries = 0;
        while ( (gzipEntry = gzipReader.getNextEntry()) != null ) {
          in = new ByteCountingPushBackInputStream( new BufferedInputStream( gzipEntry.getInputStream(), 8192 ), 32 );
          //in2 = new ByteCountingPushBackInputStream(new FileInputStream(file), 32);
          GZIPInputStream gzInputStream = null;
          gzInputStream = new GZIPInputStream(new FileInputStream(file));
          warcReader2 = WarcReaderFactory.getReaderCompressed();
          in2 = new ByteCountingPushBackInputStream(gzInputStream, 32);
          ++gzipEntries;
          //System.out.println(gzipEntries + " - " + gzipEntry.getStartOffset() + " (0x" + (Long.toHexString(gzipEntry.getStartOffset())) + ")");
          if ( gzipEntries == 1 ) {
            if ( ArcReaderFactory.isArcFile( in ) ) {
              arcReader = ArcReaderFactory.getReaderUncompressed();
              arcReader.setUriProfile(uriProfile);
              arcReader.setBlockDigestEnabled( bBlockDigestEnabled );
              arcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
              arcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
              arcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
            }
            else if ( WarcReaderFactory.isWarcFile( in ) ) {
              warcReader = WarcReaderFactory.getReaderUncompressed();
              warcReader.setWarcTargetUriProfile(uriProfile);
              warcReader.setBlockDigestEnabled( bBlockDigestEnabled );
              warcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
              warcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
              warcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
            }
            else {
            }
          }
          if ( arcReader != null ) {
            while ( (arcRecord = arcReader.getNextRecordFrom( in, gzipEntry.getStartOffset() )) != null ) {
            }
          }
          else if ( warcReader != null ) {
            while ( (warcRecord = warcReader.getNextRecordFrom( in2, 0 ) ) != null ) {
            //while ( (warcRecord = warcReader.getNextRecordFrom( in, gzipEntry.getStartOffset() ) ) != null ) {
            //while ( (warcRecord = warcReader2.getNextRecordFrom(in2, 0) ) != null ) {
              uri = warcRecord.header.warcTargetUriStr;
              System.out.println(uri);
              key = Util.reverseHostname(uri);
              if(key != null && key.equals("gov.house.www/")){
                System.out.println("##################################################################");
                System.out.println("at " + file.getName());
                System.out.println("Not added yet");
              }
            }
          }
          else {
            while ( in.read(buffer) != -1 ) {
            }
          }
          in.close();
          gzipEntry.close();
        }
      }
      else if ( ArcReaderFactory.isArcFile( pbin ) ) {
        arcReader = ArcReaderFactory.getReaderUncompressed( pbin );
        arcReader.setUriProfile(uriProfile);
        arcReader.setBlockDigestEnabled( bBlockDigestEnabled );
        arcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
        arcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
        arcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
        while ( (arcRecord = arcReader.getNextRecord()) != null ) {
        }
        arcReader.close();
      }
      else if ( WarcReaderFactory.isWarcFile( pbin ) ) {
        warcReader = WarcReaderFactory.getReaderUncompressed( pbin );
        warcReader.setWarcTargetUriProfile(uriProfile);
        warcReader.setBlockDigestEnabled( bBlockDigestEnabled );
        warcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
        warcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
        warcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
        System.err.println("Uncompressed.");
        while ( (warcRecord = warcReader.getNextRecord()) != null ) {
          uri = warcRecord.header.warcTargetUriStr;
          System.out.println(uri);
          key = Util.reverseHostname(uri);
          if(key != null && key.equals("gov.house.www/")){
            System.out.println("##################################################################");
            System.out.println("at " + file.getName());
            System.out.println("Added.");
          }
        }
        warcReader.close();
      }
      else {
      }
    }
    catch (Throwable t) {
      // TODO just use reader.getStartOffset?
      long startOffset = -1;
      Long length = null;
      if (arcRecord != null) {
        startOffset = arcRecord.getStartOffset();
        length = arcRecord.header.archiveLength;
      }
      if (warcRecord != null) {
        startOffset = warcRecord.getStartOffset();
        length = warcRecord.header.contentLength;
      }
      if (gzipEntry != null) {
        startOffset = gzipEntry.getStartOffset();
        // TODO correct entry size including header+trailer.
        length = gzipEntry.compressed_size;
      }
      if (length != null) {
        startOffset += length;
      }
    }
    finally {
      if ( arcReader != null ) {
        arcReader.close();
      }
      if ( warcReader != null ) {
        warcReader.close();
      }
      if (gzipReader != null) {
        try {
          gzipReader.close();
        }
        catch (IOException e) {
        }
      }
      if (pbin != null) {
        try {
          pbin.close();
        }
        catch (IOException e) {
        }
      }
      if (raf != null) {
        try {
          raf.close();
        }
        catch (IOException e) {
        }
      }
    }
    return pbin.getConsumed();
  }
}
