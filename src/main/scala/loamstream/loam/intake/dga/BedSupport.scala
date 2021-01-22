package loamstream.loam.intake.dga

import java.net.URI

import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Source
import loamstream.util.HttpClient
import loamstream.util.SttpHttpClient
import java.io.StringReader
import java.io.InputStream
import java.util.zip.GZIPInputStream
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream
import java.io.InputStreamReader
import java.io.Reader

/**
 * @author clint
 * Jan 20, 2021
 */
trait BedSupport {
  /**
   * Download the BED file in the URL and return a Source that will produce the data it contains
   */
  def downloadBed(
      url: URI,
      auth: HttpClient.Auth,
      httpClient: HttpClient = new SttpHttpClient()): Source[DataRow] = {
    
    val extOpt = url.getPath.split("\\.").lastOption.map(_.trim.toLowerCase)

    /*# determine the type of compression from the extension
    if ext in ['.gz', '.gzip']:
        ext = 'gzip'
    elif ext in ['.bz2', '.zip', '.xz']:
        ext = ext[1:]
    else:
        ext = None*/
    
    def bedReader: Reader = {
      //download the source into memory
      val bedData = httpClient.getAsBytes(url.toString, Some(auth)).right.getOrElse {
        throw new Exception(s"HTTP request failed: GET ${url}")
      }
      
      /*resp = requests.get(url, auth=(auth['username'], auth['password']))
      if resp.status_code != 200:
          raise Exception('HTTP request error: %s' % resp.reason)*/
  
      /*# parse the content
      bed = io.BytesIO(resp.content)*/
  
      val bedStream = new ByteArrayInputStream(bedData)
      
      val unzippedBedStream: InputStream = extOpt match {
        case Some("gz") | Some("gzip") => new GZIPInputStream(bedStream)
        case Some("bz2") => new BZip2CompressorInputStream(bedStream)
        case Some("zip") => new ZipInputStream(bedStream)
        case Some("xz") => new XZCompressorInputStream(bedStream)
        case _ => bedStream
      }
      
      new InputStreamReader(unzippedBedStream)
    }
    
    //TODO: Header?
    Source.fromReader(bedReader, Source.Formats.spaceDelimited)
    
    /*# read the response into a frame
    return pandas.read_csv(
        bed,
        compression=ext,
        index_col=None,
        delim_whitespace=True,
        keep_default_na=True,
        na_values=['.'],
    )
    
    ???*/
  }
}
