import io.minio.*

import java.io.{ByteArrayInputStream, InputStream }

import scala.jdk.CollectionConverters._

object Minio {


  val minioClient = MinioClient.builder
    .endpoint("http://localhost:9000")
    .credentials(
      // generated at http://localhost:.../access-keys/new-account
      "J3ZPGJNYjja3z74T",
      "Gs82a3wW6APexLBT2cdQjXlmCwRa7mqk"
    )
    .build()

  import minioClient._

  def bucketExists(bucketName: String): Boolean =
    minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())

  /**
   * Put object into minio storage
   *
   * @param bucket bucket name
   * @param id     object id
   * @param blob   object blob
   */
  def put(bucketName: String, id: String, inputStream: InputStream, size: Long, contentType: String = "binary/octet-stream"): Unit = {

    // create bucket if not exists
    if (!bucketExists(bucketName)) {
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
  def get(bucketName: String, id: String): GetObjectResponse /* extends FilterInputStream */ = 
    getObject(
      GetObjectArgs.builder
        .bucket(bucketName)
        .`object`(id)
        .build
    )


  /**
   * remove object from minio storage
   *
   * @param bucket bucket name
   * @param id     object it
   */
  def delete(bucketName: String, id: String): Unit = {
    removeObject(
      RemoveObjectArgs.builder()
        .bucket(bucketName)
        .`object`(id)
        .build()
    )

    println("bucket size\t= " + listObjects(ListObjectsArgs.builder().bucket(bucketName).build()).asScala.size)
  }

}
