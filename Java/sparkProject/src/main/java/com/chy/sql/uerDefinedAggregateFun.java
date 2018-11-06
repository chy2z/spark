package com.chy.sql;


import com.chy.util.SparkUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
* @Title: uerDefinedAggregateFun
* @Description: 用户自定义聚合函数
* @author chy
* @date 2018/5/19 15:13
*/
public class uerDefinedAggregateFun {

    public static void main(String[] arg){

        SparkSession spark= SparkUtil.getSparkSession();

        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = spark.read().json("src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();

        spark.stop();
    }


    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            /**
             * 聚合列
             */
            List<StructField> inputFields = new ArrayList<>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            /**
             * 聚合函数
             */
            List<StructField> bufferFields = new ArrayList<>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

        @Override
        public StructType inputSchema() {
            return inputSchema;
        }

        @Override
        public StructType bufferSchema() {
            return bufferSchema;
        }

        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        /**
         * 一致性检验，如果为true,那么输入不变的情况下计算的结果也是不变的。
         * @return
         */
        @Override
        public boolean deterministic() {
            return true;
        }

        /**
         * 初始化聚合值
         * @param buffer
         */
        @Override
        public void initialize(MutableAggregationBuffer buffer) {
            buffer.update(0, 0L);
            buffer.update(1, 0L);
        }

        /**
         * 更新聚合值
         * @param buffer 聚合累计值
         * @param input  行记录
         */
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {
            if (!input.isNullAt(0)) {
                /**
                 * 求和
                 */
                long updatedSum = buffer.getLong(0) + input.getLong(0);
                /**
                 * 求行数
                 */
                long updatedCount = buffer.getLong(1) + 1;
                /**
                 * 更新值
                 */
                buffer.update(0, updatedSum);
                buffer.update(1, updatedCount);
            }
        }

        /**
         * 合并聚合值（多个分区合并）
         * @param buffer1
         * @param buffer2
         */
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
            long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
            long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
            buffer1.update(0, mergedSum);
            buffer1.update(1, mergedCount);
        }

        /**
         * 最终结果
         * @param buffer
         * @return
         */
        @Override
        public Object evaluate(Row buffer) {
            /**
             * 总和除以总行数
             */
            return ((double) buffer.getLong(0)) / buffer.getLong(1);
        }
    }
}