package org.sparkbyexample.sparkscala.dataframe.examples

class Util extends Serializable {
  def combine(fname:String,mname:String,lname:String):String = {
    fname+","+mname+","+lname
  }
}
