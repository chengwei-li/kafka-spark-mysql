package com.studysai.lamda.webService

import javax.xml.bind.annotation.XmlRootElement

/**
  * Created by lj on 2017/5/18.
  */
@XmlRootElement
class RecoItem {

  private var item : Array[Long] = null

  def getItem: Array[Long] = {
    return item
  }
  def setItem (item : Array[Long]): Unit ={
    this.item = item
  }
}
