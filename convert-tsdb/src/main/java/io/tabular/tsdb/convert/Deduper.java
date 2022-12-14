package io.tabular.tsdb.convert;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import lombok.SneakyThrows;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class Deduper {

  private static final String TS_FORMAT = "yyyy-MM-dd HH:mm:ss'Z'";
  private static final String CREATE_TABLE_SQL =
      "CREATE TABLE reading_archive.tsdb_import_current_dedupe ("
          + " metric STRING,"
          + " time TIMESTAMP,"
          + " value DOUBLE,"
          + " lsp_id INT,"
          + " circuit_id INT,"
          + " power_sign STRING,"
          + " tags MAP<STRING, STRING>,"
          + " version BIGINT,"
          + " idx INT)"
          + " USING iceberg"
          + " PARTITIONED BY (days(time))";

  private static final String ALTER_TABLE_SQL =
      "ALTER TABLE reading_archive.tsdb_import_current_dedupe"
          + " WRITE ORDERED BY metric, lsp_id, circuit_id, time";

  private final SparkSession spark;
  private final boolean dryRun;
  private final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern(TS_FORMAT).withZone(ZoneOffset.UTC);

  @SneakyThrows
  public static void main(String[] args) {
    Options options = new Options();
    options.addOption("y", "dry-run", false, "Run process without writing to table");

    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);

    boolean dryRun = cmd.hasOption("y");

    SparkConf conf = new SparkConf().setAppName("deduper");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

    new Deduper(spark, dryRun).run();

    System.out.println("******* Dedupe complete");
  }

  public Deduper(SparkSession spark, boolean dryRun) {
    this.spark = spark;
    this.dryRun = dryRun;
  }

  public void run() {
    System.out.println("Creating table...");
    System.out.println(CREATE_TABLE_SQL);
    System.out.println(ALTER_TABLE_SQL);
    if (!dryRun) {
      spark.sql(CREATE_TABLE_SQL);
      spark.sql(ALTER_TABLE_SQL);
    }

    Instant start = Instant.parse("2021-03-01T00:00:00Z");
    Instant last = Instant.parse("2022-12-01T00:00:00Z");

    runInsert(format("time<'%s'", timeFormat(start)));

    while (start.isBefore(last)) {
      Instant end = start.plus(10, ChronoUnit.DAYS);
      runInsert(format("time>='%s' and time<'%s'", timeFormat(start), timeFormat(end)));
      start = end;
    }

    runInsert(format("time>='%s'", timeFormat(start)));
  }

  private String timeFormat(Instant instant) {
    return formatter.format(instant);
  }

  @SneakyThrows
  private void runInsert(String filter) {
    System.out.println("Running dedupe with filter: " + filter);

    if (!dryRun) {
      WindowSpec window =
          Window.partitionBy(
                  "time", "metric", "lsp_id", "circuit_id", "power_sign", "value", "version", "idx")
              .orderBy("time");

      spark
          .read()
          .option("vectorization-enabled", false)
          .table("reading_archive.tsdb_import_current")
          .withColumn("rownum", row_number().over(window))
          .filter("rownum=1 and " + filter)
          .drop("rownum")
          .writeTo("reading_archive.tsdb_import_current_dedupe")
          .append();
    }
  }
}
