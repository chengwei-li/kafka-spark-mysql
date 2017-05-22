package com.studysai.lamda.webService

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.servlet.ServletContainer
/**
  * Created by lj on 2017/5/18.
  *
  *
  */
object RecoServer {

  def start: Unit ={
    val context : ServletContextHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    context.setContextPath("/")
    val webServer = new Server(9999)
    val jerseyServlet : ServletHolder = context.addServlet(classOf[ServletContainer], "/*")
    jerseyServlet.setInitOrder(0)
    jerseyServlet.setInitParameter("jersey.config.server.provider.packages", "com.studysai.lamda.webService")
    try {
      webServer.start()
      webServer.join()
    } catch {
      case e : Exception => {
        println(e.getMessage)
      }
      case e : Throwable => println(e.getMessage)
    }finally {
      webServer.destroy()
    }
  }

  def main(args: Array[String]): Unit = {
    RecoServer.start
  }
}
