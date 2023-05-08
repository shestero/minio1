import io.minio.*

import java.io.{ByteArrayInputStream, InputStream }

import scala.jdk.CollectionConverters._

object Minio {


  val minioClient = MinioClient.builder
    .endpoint("http://localhost:9000")
    .credentials(
      // generated at http://localhost:40965/access-keys/new-account
      "J3ZPGJNYjja3z74T",
      "Gs82a3wW6APexLBT2cdQjXlmCwRa7mqk"
      // "3vfMBGgVTAP3yXwwgzSOWlDiXSDqUEDR" // "npUFmHEvul3Lle19lOWITCFeLKr0KCDQ"
    )
    .build()

  import minioClient._


  /**
   * Put object into minio storage
   *
   * @param bucket bucket name
   * @param id     object id
   * @param blob   object blob
   */
  def put(bucketName: String, id: String, inputStream: InputStream, size: Long, contentType: String = "binary/octet-stream"): Unit = {

    // create bucket if not exists
    val argExist = BucketExistsArgs.builder().bucket(bucketName).build()
    if (!bucketExists(argExist)) {
      makeBucket(
        MakeBucketArgs.builder()
          .bucket(bucketName)
          .build()
      )
    }

    // put object
    putObject(
      PutObjectArgs.builder
        .bucket(bucketName)
        .`object`(id)
        .stream(inputStream, size, -1)
        .contentType(contentType)
        .build
    )

    inputStream.close()
  }

  /**
   * Get object from minio storage
   *
   * @param bucket bucket name
   * @param id     object it
   */
  def get(bucketName: String, id: String): GetObjectResponse /* extends FilterInputStream */ = {
    val stream: GetObjectResponse = getObject(
      GetObjectArgs.builder
        .bucket(bucketName)
        .`object`(id)
        .build
    )

    // get object as byte array
    //val blob = IOUtils.toByteArray(stream)
    //println(blob.length)

    // get object stat
    val stat = statObject(
      StatObjectArgs.builder()
        .bucket(bucketName)
        .`object`(id)
        .build()
    )
    println("size\t= " + stat.size())
    println("versionId\t= " + stat.versionId())
    println("bucket\t= " + stat.bucket())

    stream
  }

  /**
   * remove object from minio storage
   *
   * @param bucket bucket name
   * @param id     object it
   */
  def delete(bucketName: String, id: String): Unit = {
    // remove object
    removeObject(
      RemoveObjectArgs.builder()
        .bucket(bucketName)
        .`object`(id)
        .build()
    )

    // get size of the bucket
    println("buckets count\t= " + listObjects(ListObjectsArgs.builder().bucket(bucketName).build()).asScala.size)
  }

}
