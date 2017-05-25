/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.spark.testsuite.commands

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties

class SetCommandTestCase  extends QueryTest with BeforeAndAfterAll {

  test("test set command") {

    sql("set spark.eventLog.enabled=2").show(false)
    sql("set spark.executor.memory=manu").show(false)
    sql("set").show(false)
    sql("set -v").show(false)
//    sql("drop table if exists abc")
//    sql("create table abc(name string, value string) stored by 'carbondata'")
//    sql("set carbon.enable.vector.reader=false")
//    sql("insert into abc select 'manu', '123'")
//    sql(
//      "CREATE table abc (ID int, date String, country String, name " +
//      "String," +
//      "phonetype String, serialname String, salary int) stored by 'org.apache.carbondata.format'"
//
//    )
//    sql("set carbon.enable.vector.reader=false").show(false)
//    sql("set").show(false)
//    sql("set carbon.enable.vector.reader").show(false)
   /* sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/dataretention1.csv' INTO TABLE abc " +
        "OPTIONS('DELIMITER' =  ',')")
    sql("select * from abc").show(false)
    //    sql("set key1=value1")
//    sql("select * from abc").show(false)
    sql("DELETE SEGMENT 0 FROM TABLE abc")*/

//    assert(CarbonProperties.getInstance().getProperty("key1").equals("value1"), "Set command does not work" )
//    assert(sqlContext.getConf("key1").equals("value1"), "Set command does not work" )
  }

}
