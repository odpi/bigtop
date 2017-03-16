/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.odpi.specs.runtime.hive;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.Assert;

public class TestCLI {
	
	static Map<String, String> results;

	@BeforeClass
	public static void setup(){
		
		results = HiveHelper.execCommand(new CommandLine("which").addArgument("hive"));
		Assert.assertEquals("Hive is not in the current path.", 0, Integer.parseInt(results.get("exitValue")));
	}
	
	@Test
	public void help(){
	  results = execHive("-H");
		Assert.assertEquals("Error in executing 'hive -H'", 2, Integer.parseInt(results.get("exitValue")));
		
		results = execHive("--help");
		Assert.assertEquals("Error in executing 'hive --help'", 0, Integer.parseInt(results.get("exitValue")));
		
		results = execHive("-U");
		Assert.assertEquals("Unrecognized option should exit 1.", 1, Integer.parseInt(results.get("exitValue")));
	}
	 
	@Test
	public void sqlFromCmdLine(){

	  results = execHive("-e", "SHOW DATABASES");
		Assert.assertEquals("SHOW DATABASES command failed to execute.", 0, Integer.parseInt(results.get("exitValue")));
		if(!results.get("outputStream").contains("odpi_runtime_hive")){
      results = execHive("-e", "CREATE DATABASE odpi_runtime_hive");
			Assert.assertEquals("Could not create database odpi_runtime_hive.", 0, Integer.parseInt(results.get("exitValue")));
		}else{
			results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
			results = execHive("-e", "CREATE DATABASE odpi_runtime_hive");
			Assert.assertEquals("Could not create database odpi_runtime_hive.", 0, Integer.parseInt(results.get("exitValue")));
		}
		results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
	}
	
	@Test
	public void sqlFromFiles() throws FileNotFoundException{
		try(PrintWriter out = new PrintWriter("hive-f1.sql")){ out.println("SHOW DATABASES;"); }
		try(PrintWriter out = new PrintWriter("hive-f2.sql")){ out.println("CREATE DATABASE odpi_runtime_hive;"); }
		try(PrintWriter out = new PrintWriter("hive-f3.sql")){ out.println("DROP DATABASE odpi_runtime_hive;"); out.println("CREATE DATABASE odpi_runtime_hive;"); }
		try(PrintWriter out = new PrintWriter("hive-f4.sql")){ out.println("DROP DATABASE odpi_runtime_hive;"); }
		results = execHive("-f", "hive-f1.sql");
		Assert.assertEquals("SHOW DATABASES command failed to execute.", 0, Integer.parseInt(results.get("exitValue")));
		if(!results.get("outputStream").contains("odpi_runtime_hive")){
			results = execHive("-f", "hive-f2.sql");
			Assert.assertEquals("Could not create database odpi_runtime_hive.", 0, Integer.parseInt(results.get("exitValue")));
		}else{
			results = execHive("-f", "hive-f3.sql");
			Assert.assertEquals("Could not create database odpi_runtime_hive.", 0, Integer.parseInt(results.get("exitValue")));
		}
		results = execHive("-f", "hive-f4.sql");
	}
	
	@Test
	public void silent() {
		results = execHive("-e", "SHOW DATABASES", "-S");
		Assert.assertEquals("-S option did not work.", false, results.get("outputStream").contains("Time taken:"));
		
		results = execHive("-e", "SHOW DATABASES", "--silent");
		Assert.assertEquals("--silent option did not work.", false, results.get("outputStream").contains("Time taken:"));
	}
	
	@Test
	public void verbose(){
		results = execHive("-e", "SHOW DATABASES", "-v");
		Assert.assertEquals("-v option did not work.", true, results.get("outputStream").contains("SHOW DATABASES"));
		
		results = execHive("-e", "SHOW DATABASES", "--verbose");
		Assert.assertEquals("--verbose option did not work.", true, results.get("outputStream").contains("SHOW DATABASES"));
	}
	
	@Test
	public void initialization() throws FileNotFoundException{
		try(PrintWriter out = new PrintWriter("hive-init1.sql")){ out.println("CREATE DATABASE odpi_runtime_hive;"); }
		try(PrintWriter out = new PrintWriter("hive-init2.sql")){ out.println("DROP DATABASE odpi_runtime_hive;"); out.println("CREATE DATABASE odpi_runtime_hive;"); }
		
		results = execHive("-e", "SHOW DATABASES");
		Assert.assertEquals("SHOW DATABASES command failed to execute.", 0, Integer.parseInt(results.get("exitValue")));
		if(!results.get("outputStream").contains("odpi_runtime_hive")){
			results = execHive("-i", "hive-init1.sql", "-e", "SHOW DATABASES");
			Assert.assertEquals("Could not create database odpi_runtime_hive using the init -i option.", 0, Integer.parseInt(results.get("exitValue")));
			Assert.assertEquals("Could not create database odpi_runtime_hive using the init -i option.", true, results.get("outputStream").contains("odpi_runtime_hive"));
		}else{
			results = execHive("-i", "hive-init2.sql", "-e", "SHOW DATABASES");
			Assert.assertEquals("Could not create database odpi_runtime_hive.", 0, Integer.parseInt(results.get("exitValue")));
			Assert.assertEquals("Could not create database odpi_runtime_hive using the init -i option.", true, results.get("outputStream").contains("odpi_runtime_hive"));
		}
		results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
	}
	
	@Test
	public void database(){

    results = execHive("-e", "CREATE DATABASE if not exists odpi_runtime_hive");
		results = execHive("--database", "odpi_runtime_hive_1234", "-e", "CREATE TABLE odpi ( MYID INT );");
		Assert.assertEquals("Non-existent database returned with wrong exit code: "+Integer.parseInt(results.get("exitValue")), 88, Integer.parseInt(results.get("exitValue")));
		
		results = execHive("--database", "odpi_runtime_hive", "-e", "CREATE TABLE odpi ( MYID INT );");
		Assert.assertEquals("Failed to create table using --database argument.", 0, Integer.parseInt(results.get("exitValue")));
		
		results = execHive("--database", "odpi_runtime_hive", "-e", "DESCRIBE odpi");
		Assert.assertEquals("Failed to get expected column after creating odpi table using --database argument.", true, results.get("outputStream").contains("myid"));
		
		results = execHive("--database", "odpi_runtime_hive", "-e", "DROP TABLE odpi");
		Assert.assertEquals("Failed to create table using --database argument.", 0, Integer.parseInt(results.get("exitValue")));
		
		results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
	}
	
	@Test
	public void hiveConf(){
		results = execHive("--hiveconf", "hive.root.logger=INFO,console", "-e", "SHOW DATABASES");
		Assert.assertEquals("The --hiveconf option did not work in setting hive.root.logger=INFO,console.", true, results.get("outputStream").contains("ObjectStore, initialize called"));
	}
	
	@Test
	public void variableSubsitution() throws FileNotFoundException{
    results = execHive("-e", "CREATE DATABASE if not exists odpi_runtime_hive");
		try(PrintWriter out = new PrintWriter("hive-define.sql")){ out.println("show ${A};"); out.println("quit;"); }
		results = execHive("-d", "A=DATABASES", "-f", "hive-define.sql");
		Assert.assertEquals("The hive -d A=DATABASES option did not work.", 0, Integer.parseInt(results.get("exitValue")));
		Assert.assertEquals("The hive -d A=DATABASES option did not work.", true, results.get("outputStream").contains("odpi_runtime_hive"));
		
		results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
	}
	
	@Test
	public void hiveVar() throws FileNotFoundException{
    results = execHive("-e", "CREATE DATABASE if not exists odpi_runtime_hive");
		try(PrintWriter out = new PrintWriter("hive-var.sql")){ out.println("show ${A};"); out.println("quit;"); }
		results = execHive("--hivevar", "A=DATABASES", "-f", "hive-var.sql");
		Assert.assertEquals("The hive --hivevar A=DATABASES option did not work.", 0, Integer.parseInt(results.get("exitValue")));
		Assert.assertEquals("The hive --hivevar A=DATABASES option did not work.", true, results.get("outputStream").contains("odpi_runtime_hive"));
		
		try(PrintWriter out = new PrintWriter("hiveconf-var.sql")){ out.println("show ${hiveconf:A};"); out.println("quit;"); }
		results = execHive("--hiveconf", "A=DATABASES", "-f", "hiveconf-var.sql");
		Assert.assertEquals("The hive --hiveconf A=DATABASES option did not work.", 0, Integer.parseInt(results.get("exitValue")));
		Assert.assertEquals("The hive --hiveconf A=DATABASES option did not work.", true, results.get("outputStream").contains("odpi_runtime_hive"));
		
		results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
	}
	
	@AfterClass
	public static void cleanup(){
		results = execHive("-e", "DROP DATABASE odpi_runtime_hive");
		results = HiveHelper.execCommand(new CommandLine("/bin/sh").addArgument("-c").addArgument("rm -rf hive-f*.sql", false));
		results = HiveHelper.execCommand(new CommandLine("/bin/sh").addArgument("-c").addArgument("rm -rf hive-init*.sql", false));
		results = HiveHelper.execCommand(new CommandLine("/bin/sh").addArgument("-c").addArgument("rm -rf hive-define.sql", false));
		results = HiveHelper.execCommand(new CommandLine("/bin/sh").addArgument("-c").addArgument("rm -rf hive-var.sql", false));
		results = HiveHelper.execCommand(new CommandLine("/bin/sh").addArgument("-c").addArgument("rm -rf hiveconf-var.sql", false));
	}

  private static Map<String, String> execHive(String... args) {
	  return HiveHelper.execCommand(new CommandLine("hive")
        .addArguments(args)
        .addArgument("--hiveconf")
        .addArgument("datanucleus.schema.autoCreateAll=true")
        .addArgument("--hiveconf")
        .addArgument("hive.metastore.schema.verification=false")
        .addArgument("--hiveconf")
        .addArgument("javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=odpi_metastore_db;create=true"));
  }
}
