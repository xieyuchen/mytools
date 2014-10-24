package com.meituan.mthdp.dfsusage

import java.security.PrivilegedAction
import java.text.{SimpleDateFormat, DecimalFormat}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import com.meituan.hadoop.SecurityUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.eclipse.jetty.server.handler.AbstractHandler
import org.eclipse.jetty.server.{Request, Server}

class DfsUsage extends AbstractHandler {

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  def handle(target: String,
  baseRequest: Request,
  request: HttpServletRequest,
  response: HttpServletResponse): Unit = {
    println(target)
    response.setContentType("text/html;charset=utf-8")
    response.setStatus(HttpServletResponse.SC_OK)
    baseRequest.setHandled(true)

    response.getWriter.println(getDirContent(target))

  }

  def getDirContentWithCache(path: String): String = {
    " "
  }

  def getDirContent(path: String): String = {
    val curPath = new Path(path)
    val subdirs = fs.listStatus(curPath)
    val subcs = subdirs.map(p => {
      val cs = fs.getContentSummary(p.getPath)
      (Path.getPathWithoutSchemeAndAuthority(p.getPath), cs.getLength, p.getModificationTime)
    })
    val df = new DecimalFormat()
    val tf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val tabledata = subcs.sortBy(cs => - cs._2).map(cs =>
      <tr>
        <td>
          <a href={cs._1.toString}>{cs._1}</a>
        </td>
        <td>{df.format(cs._2).toString}</td>
        <td>{tf.format(cs._3).toString}</td>
      </tr> )

    val res =
      <table>
        <tbody>
          <tr>
            <td>
              <a href={Path.getPathWithoutSchemeAndAuthority(curPath.getParent).toString}>..</a>
            </td>
            <td></td>
            <td></td>
          </tr>
          {tabledata}
        </tbody>
      </table>

    res.toString()
  }
}

object DfsUsage {
  def main(args: Array[String]) {

    SecurityUtils.doAs("hdfs", null, new PrivilegedAction[Unit] {
      def run() = {
        val server = new Server(8989)
        val handler = new DfsUsage

        server.setHandler(handler)
        server.start()
        server.join()
      }
    })

  }
}