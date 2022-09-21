import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable.ListBuffer


/**
 * @author bahsk
 * @createTime 2022-06-20 9:25
 * @description
 */
object HadoopUtil extends App {

  private val LOGGER = LoggerFactory.getLogger(HadoopUtil.getClass)
  private val fileSystem = initFileSystem()


  /**
   * 判断文件是否存在
   *
   * @param hdfsPath 传入待判断路径
   * @return
   */
  def isExists(hdfsPath: String): Boolean = {
    //获取配置
    val fsPath: Path = new Path(hdfsPath)
    //判断路径是否存在
    val isExits = fileSystem.exists(fsPath)
    //    fileSystem.close()
    isExits
  }

  /**
   * 判断该路径下是否有其他文件
   *
   * @param hdfsPath 需要查看文件是否存在的路径
   * @return
   */
  def isExists(hdfsPath: String, isFile: Boolean): Boolean = {
    var isExists = false
    if (isFile) {

      val fileArray = getHDFSFiles(hdfsPath, isRecursive = false)
      if (fileArray.length > 1) {
        isExists = true
      }
      isExists
    } else {
      throw new FileNotFoundException("No Such File!!!")
    }

  }


  /**
   * 根据传入路径判断路径下文件夹的数据文件是否为空
   *
   * @param hdfsPath hdfs文件夹对应路径
   * @return
   */
  def isNull(hdfsPath: String): Boolean = {
    var isNull = true;
    val fileStatusArr: Array[FileStatus] = getHDFSDir(hdfsPath)

    fileStatusArr.foreach(
      x => if (x.getLen > 0) {
        isNull = false;
      }
    )
    isNull
  }




  def isNotNull(hdfsPath: String*): Boolean = {
    val ret = true
    hdfsPath.foreach(
      x =>
        if (isNull(x)) {
          throw new FileNotFoundException(s"${x} has no files ! Please ask for admin !")
        }
    )
    ret
  }


  /**
   * 批量创建文件夹
   *
   * @param path 批量创建文件
   */
  def mkdir(path: String*): Unit = {
    val executor: ExecutorService = Executors.newFixedThreadPool(10)
    path.foreach(
      x => {
        if (!isExists(x)) {
          executor.submit(new Runnable() {
            override def run(): Unit = {
              val threadName = Thread.currentThread().getName
              LOGGER.info(s" ${threadName}: To mkdir ${x}")
              fileSystem.mkdirs(new Path(x))

            }
          })
        }
      }
    )
    Thread.sleep(3)
    executor.shutdown()

  }



  /**
   * 批量拷贝
   * @param src 需要拷贝的文件的完整路径
   * @param dst 需要批量写入的地址
   */
  def copy(src: String, dst: String*): Unit = {
    val executor: ExecutorService = Executors.newFixedThreadPool(10)
    dst.foreach(
      x => {
        //如果不存在，抛出异常
        if (isExists(x)) {
          executor.submit(new Runnable() {
            override def run(): Unit = {
              val threadName = Thread.currentThread().getName
              LOGGER.info(s" ${threadName}: To copy ${x}")
              // 如果存在相同文件直接覆盖
              FileUtil.copy(fileSystem, new Path(src), fileSystem, new Path(x), false, true, fileSystem.getConf)

            }
          })
        }

      }
    )
    Thread.sleep(3)
    executor.shutdown()
  }


  /**
   * 批量拷贝文件
   *
   * @param src     /data/temp/<database>/<table>
   * @param dst     /usr/hive/warehouse/<database>/<table>/pt_d=
   * @param newName 需要替换文件名  pt_d=
   * @param suffix  输入和输出文件路径的后缀 ,需要同步的时间数组或者其他数组 [202201,202202,.....]
   */
  def copy(src: String, dst: String, newName: String, suffix: Array[String]) = {
    val executor: ExecutorService = Executors.newFixedThreadPool(10)
    suffix.foreach(
      x => {
        //如果不存在，抛出异常
        if (isExists(src + "/" + x) || isExists(src + "/" + newName + x)) {
          executor.submit(new Runnable() {
            override def run(): Unit = {
              val threadName = Thread.currentThread().getName
              LOGGER.info(s" ${threadName}:${src + "/" + x} To copy ${dst + "/" + x}")
              // 如果存在相同文件直接覆盖
              fileSystem.rename(new Path(src + "/" + x), new Path(src + "/" + newName + x))
              FileUtil.copy(fileSystem, new Path(src + "/" + newName + x), fileSystem, new Path(dst), false, true, fileSystem.getConf)
            }
          })
        }
      }
    )
    Thread.sleep(3)
    executor.shutdown()
  }


  /**
   * 初始化文件系统
   *
   * @return
   */
  def  initFileSystem(): FileSystem = {
    val configuration: Configuration = new Configuration()
    configuration.set("fs.defaultFS", ResourceUtil.getProperty("fs.defaultFS"))
    FileSystem.get(configuration)
  }

  /**
   * 根据需求是否递归遍历目录
   *
   * @param hdfsDirectory
   * @param isRecursive
   * @return
   */
  def getHDFSFiles(hdfsDirectory: String, isRecursive: Boolean): Array[String] = {
    //获取配置
    val fsPath: Path = new Path(hdfsDirectory)

    //递归获取文件
    val iterator = fileSystem.listFiles(fsPath, isRecursive)
    val list = new ListBuffer[String]
    while (iterator.hasNext) {
      val pathStatus = iterator.next()
      val hdfsPath = pathStatus.getPath
      val fileName = hdfsPath.getName
      list += fileName // list.append(fileName)
    }
    list.toArray
  }

  /**
   * 返回指定目录下的
   *
   * @param hdfsDirectory
   * @return 目录,是否文件夹(true:是)
   */
  def getHDFSDir(hdfsDirectory: String): Array[FileStatus] = {
    val fsPath: Path = new Path(hdfsDirectory)
    val fileStatuses = fileSystem.listStatus(fsPath)
    fileStatuses.map(
      ele => {
        val fileStatus = ele
        (fileStatus.getPath, fileStatus.isDirectory)
      }
    )
//    fileSystem.close()
    fileStatuses
  }

  def closeFS() = {
    fileSystem.close()
  }

  //TODO 删除指定目录
  def delete( path: String) = {
    fileSystem.delete(new Path(path),true)
  }

}
