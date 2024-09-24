package org.apache.calcite.example.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

public class CalciteSQLParser {

  public static void main(String[] args) throws SqlParseException {
    String ddl = "CREATE TABLE aa (id INT) WITH ('connector' = 'file')";
    String sql = "select ca, cb, cc from t where cast(ca AS INT) = 10 AND YEAR() > 2000";

    String expr = "1 + 1";

    SqlParser.Config config = SqlParser.config()
            .withParserFactory(SqlParserImpl.FACTORY);

    SqlParser parser = SqlParser.create(sql, config);
//    SqlNode sqlNode = parser.parseExpression();
    SqlNode sqlNode = parser.parseStmt();
    System.out.println(sqlNode);
  }
}
