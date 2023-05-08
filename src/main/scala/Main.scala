import java.io.ByteArrayInputStream

import scala.util.Try
import cats.effect.*
import cats.syntax.all.*
import org.http4s.HttpRoutes
import org.http4s.server.Router
import org.http4s.blaze.server.BlazeServerBuilder
import sttp.client3.*
import sttp.tapir.*
import sttp.tapir.files.*
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.model.MediaType.*
import sttp.model.HeaderNames.ContentType

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
 
import java.net.URI
import java.io.{ByteArrayInputStream, InputStream }
import java.nio.charset.StandardCharsets.UTF_8
import java.lang.Character.MAX_RADIX
import java.security.MessageDigest

object Main extends IOApp {

  val bucketName = "test"

  // def md5(s: String) = MessageDigest.getInstance("MD5").digest(s.getBytes)

  def sha3384(s: String, radix: Int = MAX_RADIX): String = {
    val md = MessageDigest.getInstance("SHA3-384");
    new java.math.BigInteger(1, md.digest(s.getBytes(UTF_8))).toString(radix)
  }

  val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend()

  val logic: String => IO[Either[Unit, (InputStream, String)]] = url => {
    val hash = sha3384(url)
    IO.pure(Right[Unit, (InputStream,String)](
      Try {
        val cached = Minio.get(bucketName, hash)
        println(s"Cached!\t$hash\t$url")
        cached.asInstanceOf[InputStream] -> cached.headers().get("Content-Type")
      } getOrElse {
        println(s"Not in cache:\t$hash\t$url")

        val response = basicRequest.response(asByteArray).get(uri"$url").send(backend)
        val result = response.body
        println("response.contentLength=" + response.contentLength)
        println("response.contentType=" + response.contentType)

        val contentType = response.contentType getOrElse ApplicationOctetStream.toString

        result.fold(
          msg => // error
            new ByteArrayInputStream(msg.getBytes(UTF_8)) -> TextPlainUtf8.toString,

          blob => // success
            Future {
              Minio.put(bucketName, hash, new ByteArrayInputStream(blob), blob.length, contentType)
            }.failed.foreach { e =>
              println(s"Error saving to cache ($hash): ${e.getMessage}")
            }
            new ByteArrayInputStream(blob) -> contentType
        )
      }
    ))
  }

  val options: FilesOptions[IO] =
    FilesOptions
      .default
      .withUseGzippedIfAvailable // serves file.txt.gz instead of file.txt if available and Accept-Encoding contains "gzip"
      .defaultFile(List("index.html"))

  val rootEndpoint = staticFilesGetServerEndpoint(emptyInput)("./www", options)

  val cacheEndpoint =
    endpoint.description("get from cached url")
      .get
      .in("cache" / query[String]("url"))
      .out(inputStreamBody)
      .out(header(ContentType)(Codec.listHead(Codec.string))) // dynamic content type
      .serverLogic(logic)

  /*
  // delete object
  println("delete")
  Minio.delete("bucketName", "01234")
  */

  val routes: HttpRoutes[IO] = Http4sServerInterpreter[IO]().toRoutes( List(cacheEndpoint, rootEndpoint) )

  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  override def run(args: List[String]): IO[ExitCode] =
    BlazeServerBuilder[IO]
      .withExecutionContext(ec)
      .bindHttp(9090, "0.0.0.0")
      .withHttpApp(Router("/" -> routes).orNotFound)
      .resource
      .useForever
      .as(ExitCode.Success)
}
