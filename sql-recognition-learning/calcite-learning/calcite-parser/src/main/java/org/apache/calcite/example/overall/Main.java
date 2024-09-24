package org.apache.calcite.example.overall;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.example.CalciteUtil;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Arguments: <br>
 * args[0]: csv file path for user.csv <br>
 * args[1]: csv file path for order.csv <br>

 * 这段代码的主要作用和目标是：
 * 从资源文件夹中读取用户和订单的 CSV 文件路径，并创建相应的表。
 * 使用 Calcite 库定义 SQL 查询，并通过解析、验证、转换和优化生成查询计划。
 * 执行生成的查询计划并输出查询结果。
 */
public class Main {

    public static void main(String[] args) throws Exception {
        // 获取 resources 文件夹中的 user.csv 文件路径并转换为 Path 对象
        Path userPath = Paths.get(Objects.requireNonNull(Main.class.getResource("/user.csv")).toURI());
        // 获取 resources 文件夹中的 order.csv 文件路径并转换为 Path 对象
        Path orderPath = Paths.get(Objects.requireNonNull(Main.class.getResource("/order.csv")).toURI());

        // 创建表示 user 表的 SimpleTable 对象
        SimpleTable userTable = SimpleTable.newBuilder("users")
                .addField("id", SqlTypeName.VARCHAR)  // 添加 id 字段，类型为 VARCHAR
                .addField("name", SqlTypeName.VARCHAR)  // 添加 name 字段，类型为 VARCHAR
                .addField("age", SqlTypeName.INTEGER)  // 添加 age 字段，类型为 INTEGER
                .withFilePath(userPath.toString())  // 设置表对应的文件路径
                .withRowCount(10)  // 设置表的行数
                .build();
        // 创建表示 order 表的 SimpleTable 对象
        SimpleTable orderTable = SimpleTable.newBuilder("orders")
                .addField("id", SqlTypeName.VARCHAR)  // 添加 id 字段，类型为 VARCHAR
                .addField("user_id", SqlTypeName.VARCHAR)  // 添加 user_id 字段，类型为 VARCHAR
                .addField("goods", SqlTypeName.VARCHAR)  // 添加 goods 字段，类型为 VARCHAR
                .addField("price", SqlTypeName.DECIMAL)  // 添加 price 字段，类型为 DECIMAL
                .withFilePath(orderPath.toString())  // 设置表对应的文件路径
                .withRowCount(10)  // 设置表的行数
                .build();
        // 创建 SimpleSchema 对象并添加 user 和 order 表
        SimpleSchema schema = SimpleSchema.newBuilder("s")
                .addTable(userTable)
                .addTable(orderTable)
                .build();
        // 创建根 Schema
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getSchemaName(), schema);

        // 定义 SQL 查询语句
        String sql = "SELECT u.id, name, age, sum(price) " +
                "FROM users AS u join orders AS o ON u.id = o.user_id " +
                "WHERE age >= 20 AND age <= 30 " +
                "GROUP BY u.id, name, age " +
                "ORDER BY u.id";
        String sql1 = "SELECT id, name, age + 1 FROM users";
        String sql2 = "INSERT INTO users VALUES (1, 'Jark', 21)";
        String sql3 = "DELETE FROM users WHERE id > 1";
        String sql4 = "SELECT u.name, o.price FROM users AS u join orders AS o " +
                "on u.id = o.user_id WHERE o.price > 90";

        // 创建优化器
        Optimizer optimizer = Optimizer.create(schema);
        // 1. SQL parse: SQL string --> SqlNode
        SqlNode sqlNode = optimizer.parse(sql1);
        CalciteUtil.print("Parse result:", sqlNode.toString());
        // 2. SQL validate: SqlNode --> SqlNode
        SqlNode validateSqlNode = optimizer.validate(sqlNode);
        CalciteUtil.print("Validate result:", validateSqlNode.toString());
        // 3. SQL convert: SqlNode --> RelNode
        RelNode relNode = optimizer.convert(validateSqlNode);
        CalciteUtil.print("Convert result:", relNode.explain());
        // 4. SQL Optimize: RelNode --> RelNode
        RuleSet rules = RuleSets.ofList(
                CoreRules.FILTER_TO_CALC,
                CoreRules.PROJECT_TO_CALC,
                CoreRules.FILTER_CALC_MERGE,
                CoreRules.PROJECT_CALC_MERGE,
                CoreRules.FILTER_INTO_JOIN,
                EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                EnumerableRules.ENUMERABLE_JOIN_RULE,
                EnumerableRules.ENUMERABLE_SORT_RULE,
                EnumerableRules.ENUMERABLE_CALC_RULE,
                EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        // 使用规则集优化查询计划
        RelNode optimizerRelTree = optimizer.optimize(
                relNode,
                relNode.getTraitSet().plus(EnumerableConvention.INSTANCE),
                rules);
        CalciteUtil.print("Optimize result:", optimizerRelTree.explain());
        // 5. SQL execute: RelNode --> execute code
        EnumerableRel enumerable = (EnumerableRel) optimizerRelTree;
        Map<String, Object> internalParameters = new LinkedHashMap<>();
        EnumerableRel.Prefer prefer = EnumerableRel.Prefer.ARRAY;
        // 将查询计划转换为可绑定的执行代码
        Bindable bindable = EnumerableInterpretable.toBindable(internalParameters,
                null, enumerable, prefer);
        // 绑定上下文执行查询
        Enumerable bind = bindable.bind(new SimpleDataContext(rootSchema.plus()));
        Enumerator enumerator = bind.enumerator();
        // 遍历查询结果并输出
        while (enumerator.moveNext()) {
            Object current = enumerator.current();
            Object[] values = (Object[]) current;
            StringBuilder sb = new StringBuilder();
            for (Object v : values) {
                sb.append(v).append(",");
            }
            sb.setLength(sb.length() - 1);
            System.out.println(sb);
        }
    }
}
