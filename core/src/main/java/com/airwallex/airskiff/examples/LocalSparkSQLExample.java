package com.airwallex.airskiff.examples;

import com.airwallex.airskiff.core.SourceStream;
import com.airwallex.airskiff.core.api.Stream;
import com.airwallex.airskiff.spark.AbstractSparkCompiler;
import com.airwallex.airskiff.spark.SparkLocalTextConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

class Something{
  public Something(){

  }

  private Long cc;

  public Something(Long cc) {
    this.cc = cc;
  }

  public Long getCc() {
    return cc;
  }

  public void setCc(Long cc) {
    this.cc = cc;
  }
}

public class LocalSparkSQLExample {


  public static void main(String[] args) {
    // read file from resources
    SparkSession spark = SparkSession.builder().master("local").appName("spark compiler")
      .getOrCreate();

    SparkLocalTextConfig config = new SparkLocalTextConfig("core/src/main/resources/localinput.txt");


    Stream<Something> op = new SourceStream<>(config)
      .flatMap(x -> Arrays.asList(x.split("\\s")), String.class)
      .map(x -> new Counter(x, 1L), Counter.class)
      .sql("SELECT c as cc FROM text",
        "text", Something.class);


    Dataset ds = new AbstractSparkCompiler(spark).compile(op);
    ds.explain();
    ds.show();
  }
}
