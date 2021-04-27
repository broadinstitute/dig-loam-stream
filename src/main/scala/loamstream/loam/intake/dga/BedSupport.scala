package loamstream.loam.intake.dga

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.io.Reader
import java.net.URI
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream

import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Source
import loamstream.util.HttpClient
import loamstream.util.DefaultHttpClient
import loamstream.util.TimeUtils
import loamstream.util.LogContext
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import loamstream.util.Loggable

/**
 * @author clint
 * Jan 20, 2021
 */
trait BedSupport { self: Loggable =>
  object Beds {
    /**
     * Download the BED file in the URL and return a Source that will produce the data it contains
     */
    def downloadBed(
        url: URI,
        auth: HttpClient.Auth,
        httpClient: HttpClient = new DefaultHttpClient())(implicit context: LogContext): Source[DataRow] = {
      
      val extOpt = url.getPath.split("\\.").lastOption.map(_.trim.toLowerCase)
  
      def tryBedReader: Try[Reader] = Try {
        val bedStream: InputStream = {
          context.info(s"Downloading ${url} ...")
          
          httpClient.getAsInputStream(url.toString, Some(auth)) match {
            case Left(msg) => throw new Exception(s"HTTP request failed: GET '${url}': ${msg}")
            case Right(stream) => stream
          }
        }
        
        val unzippedBedStream: InputStream = extOpt match {
          case Some("gz") | Some("gzip") => new GZIPInputStream(bedStream)
          case Some("bz2") => new BZip2CompressorInputStream(bedStream)
          case Some("zip") => new ZipInputStream(bedStream)
          case Some("xz") => new XZCompressorInputStream(bedStream)
          case _ => bedStream
        }
        
        new InputStreamReader(unzippedBedStream)
      }
      
      Source.producing(tryBedReader).flatMap { 
        case Success(bedReader) => Source.fromReader(bedReader, Source.Formats.tabDelimitedWithHeader)
        case Failure(e) => {
          warn(s"Error downloading '${url}': ", e)
          
          Source.empty
        }
      }
    }
  }
}
