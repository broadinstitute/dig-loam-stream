package loamstream.loam.intake.dga

import java.io.InputStream
import java.io.InputStreamReader
import java.io.Reader
import java.net.URI
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream

import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Source
import loamstream.util.DefaultHttpClient
import loamstream.util.HttpClient
import loamstream.util.LogContext
import loamstream.util.Loggable
import loamstream.util.Terminable

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
        auth: Option[HttpClient.Auth],
        httpClient: HttpClient = new DefaultHttpClient())(implicit context: LogContext): (Terminable, Source[DataRow]) = {
      
      val extOpt = url.getPath.split("\\.").lastOption.map(_.trim.toLowerCase)
  
      lazy val tryBedReader: Try[(Terminable, Reader)] = Try {
        val (streamHandle: Terminable, bedStream: InputStream) = {
          context.info(s"Downloading ${url} ...")
          
          httpClient.getAsInputStream(url.toString, auth) match {
            case Left(msg) => throw new Exception(s"HTTP request failed: GET '${url}': ${msg}")
            case Right(tuple) => tuple
          }
        }
        
        val unzippedBedStream: InputStream = extOpt match {
          case Some("gz") | Some("gzip") => new GZIPInputStream(bedStream)
          case Some("bz2") => new BZip2CompressorInputStream(bedStream)
          case Some("zip") => new ZipInputStream(bedStream)
          case Some("xz") => new XZCompressorInputStream(bedStream)
          case _ => bedStream
        }
        
        val reader = new InputStreamReader(unzippedBedStream)
        
        (Terminable.StopsComponents(reader, streamHandle.asCloseable), reader)
      }
      
      val terminable = Terminable {
        //TODO: debug or trace
        info(s"Closing reader for $url")
        
        tryBedReader.foreach { case (handle, _) => handle.stop() }
        
        info(s"Closed reader for $url")
      }
      
      terminable -> Source.producing(tryBedReader).flatMap { 
        case Success((_, bedReader)) => Source.fromReader(bedReader, Source.Formats.tabDelimitedWithHeader)
        case Failure(e) => {
          warn(s"Error downloading '${url}': ", e)
          
          Source.empty
        }
      }
    }
  }
}
