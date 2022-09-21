import com.typesafe.config.{Config, ConfigFactory}

import java.io.{FileInputStream, InputStream}
import java.net.URL
import java.util.Properties

/**
 * @author bahsk
 * @createTime 2022-06-20 10:17
 * @description
 */
object ResourceUtil {

  var props: Properties = new Properties
  var config: Config = null

  /**
   * 获取资源属性
   */
  def getResource(fileName: String): Properties = {
    val resource: URL = this.getClass.getClassLoader.getResource(fileName)
    val in: InputStream = new FileInputStream(resource.getPath)
    val props: Properties = new Properties
    props.load(in)
    in.close()
    this.props = props
    props
  }

  def getResource() = {
    this.config = ConfigFactory.load()
  }

  /**
   * 判断key是否存在
   * @param key
   * @return
   */
  def isExists(key: String): Boolean = {
    getResource()
//    this.props.containsKey(key)
    !this.config.getIsNull(key)
  }

  /**
   * 获取key对应的值
   * @param key
   * @return
   */
  def getProperty(key : String) = {
    val flag = isExists(key)
    if (flag) {
//      this.props.getProperty(key)
      this.config.getString(key)
    } else {
      null
    }
  }



}
