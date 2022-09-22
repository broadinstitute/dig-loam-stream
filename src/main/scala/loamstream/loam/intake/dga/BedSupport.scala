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
import loamstream.util.Processes

/**
 * @author clint
 * Jan 20, 2021
 */
trait BedSupport { self: Loggable =>
  object Beds {
    private def getBedStream(url: URI)(implicit context: LogContext): Try[(Terminable, InputStream)] = {
      context.info(s"Downloading ${url} ...")
          
      //WARNING: Cover the children's ears, hack of the century approaching. :( :( :( 
      //For reasons I could not unravel before I needed to ingest some data, the previous
      //workarounds for accessing DGA's bed files - see DefaultHttpClient.getAsInputStream -
      //didn't work with the Lung REST API at http://www.lungepigenome.org:8080/getLungAnnotations. :(
      //Auth is supposed to be required, which, along with following redirects, was one of the wrinkles
      //that interacted to cause problems in the best, but to my surprise, plain old wget with no
      //credentials worked.  At this point, I've opted not to ask too many questions, but something is
      //obviously wrong and needs fixing.  I'll take another stab at this during the next intake round.
      // Sheepishly, -Clint. July 1, 2021
      val tokens = Seq("wget", "--no-check-certificate", "-q", "-O", "-", url.toString)

      val attempt = loamstream.util.Processes.runAndExposeStreams(tokens)()(context)

      //TODO: Consume stderr?
      attempt.map { case (bedStream, _, handle) => (handle, bedStream) }
    }

    private abstract class LazyBedReaderFor(url: URI) {
      @volatile private[this] var initialized = false

      def isIntialized = initialized

      lazy val handleAndReader: Try[(Terminable, Reader)] = {
        val extOpt = url.getPath.split("\\.").lastOption.map(_.trim.toLowerCase)

        val result = getBedStream(url).map { case (streamHandle, bedStream) =>
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
        
        initialized = true
        
        result
      }
    }

    /**
     * Download the BED file in the URL and return a Source that will produce the data it contains
     */
    def downloadBed(
        url: URI,
        auth: Option[HttpClient.Auth],
        //FIXME: Default is hack of all hacks
        headers: Option[Seq[String]] = Some(Seq("chrom", "start", "end", "state", "value")),
        httpClient: HttpClient = new DefaultHttpClient()): (Terminable, Source[DataRow]) = {
      
      object BedReader extends LazyBedReaderFor(url)
      
      val terminable = Terminable {
        if(BedReader.isIntialized) {
          debug(s"Closing reader for $url")

          BedReader.handleAndReader.foreach { case (handle, _) => handle.stop() }

          debug(s"Closed reader for $url")
        }
      }
      
      //TODO: Allow specifying a delimiter too - just pass in a CSVFormat?
      val formats = headers match {
        case Some(hs) => Source.Formats.tabDelimited.withHeader(hs: _*)
        case None => Source.Formats.tabDelimitedWithHeader
      }

      terminable -> Source.producing(BedReader.handleAndReader).flatMap { 
        case Success((_, bedReader)) => Source.fromReader(bedReader, formats)
        case Failure(e) => {
          warn(s"Error downloading '${url}': ", e)
          
          Source.empty
        }
      }
    }
  }
}
