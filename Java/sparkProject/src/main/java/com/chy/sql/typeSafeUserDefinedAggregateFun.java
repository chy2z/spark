package com.chy.sql;

import com.chy.util.SparkUtil;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
* @Title: typeSafeUserDefinedAggregateFun
* @Description:
* @author chy
* @date 2018/5/19 16:04
*/
public class typeSafeUserDefinedAggregateFun {

    public static void main(String[] args){

        SparkSession spark= SparkUtil.getSparkSession();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        String path = "src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);
        ds.show();


        MyAverage myAverage = new MyAverage();
        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();

        spark.stop();

    }

    public static class Employee implements Serializable {
        private String name;
        private long salary;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
    }

    public static class Average implements Serializable  {
        private long sum;
        private long count;

        public Average() {

        }

        public Average(Long s ,Long c){
            this.sum=s;
            this.count=c;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }

    public static class MyAverage extends Aggregator<Employee, Average, Double> {

        @Override
        public Average zero() {
            return new Average(0L, 0L);
        }

        /**
         * 逐项相加
         * @param buffer
         * @param employee
         * @return
         */
        @Override
        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        /**
         * 不同分区合并
         * @param b1
         * @param b2
         * @return
         */
        @Override
        public Average merge(Average b1, Average b2) {
            long mergedSum = b1.getSum() + b2.getSum();
            long mergedCount = b1.getCount() + b2.getCount();
            b1.setSum(mergedSum);
            b1.setCount(mergedCount);
            return b1;
        }

        /**
         * 最终结果
         * @param reduction
         * @return
         */
        @Override
        public Double finish(Average reduction) {
            return ((double) reduction.getSum()) / reduction.getCount();
        }

        @Override
        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
}
