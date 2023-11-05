package org.bigData.Controllers;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bigData.Entities.Employee;
import org.bigData.Services.Reader.ReadCsvFileImpl;
import org.bigData.Services.Spark.SparkSessionStarter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import static org.apache.spark.sql.functions.col;


@Controller
@RequestMapping("/")
public class HomeController {

    private final SparkSessionStarter sessionStarter;
    private final ReadCsvFileImpl readCsvFile;

    @Value("${hdfs.path}")
    private String hdfsPath;

    public HomeController(SparkSessionStarter sessionStarter, ReadCsvFileImpl readCsvFile) {
        this.sessionStarter = sessionStarter;
        this.readCsvFile = readCsvFile;
    }

    @GetMapping("/display")
    public String display(Model model) {
        try {
            SparkSession session = sessionStarter.startSession();
            StructType emp_schema = new StructType()
                    .add("empno", DataTypes.IntegerType, false)
                    .add("ename", DataTypes.StringType, false)
                    .add("job", DataTypes.StringType, false)
                    .add("mgr", DataTypes.IntegerType, false)
                    .add("hiredate", DataTypes.DateType, false)
                    .add("sal", DataTypes.IntegerType, false)
                    .add("comm", DataTypes.IntegerType, false)
                    .add("deptno", DataTypes.IntegerType, false)
                    .add("img", DataTypes.StringType, false);

            StructType dept_schema = new StructType()
                    .add("deptno", DataTypes.IntegerType, false)
                    .add("dname", DataTypes.StringType, false)
                    .add("loc", DataTypes.IntegerType, false);


            Dataset<Row> emp = readCsvFile.entity(session, "emp.csv", hdfsPath, emp_schema);

            Dataset<Row> dept = readCsvFile.entity(session, "dept.csv", hdfsPath, dept_schema);

            //Self-join in employee's table to translate mgr id to ename
            Dataset<Row> df1 = emp.as("df1");
            Dataset<Row> df2 = emp.as("df2");
            emp = df1.join(df2, col("df1.empno").equalTo(col("df2.mgr")))
                    .select(col("df2.ename"), col("df1.ename").as("mgr"), col("df2.sal"),
                            col("df2.comm"), col("df2.deptno"), col("df2.img"));
            //Join emp and dept
            Dataset<Row> emp_dept = emp.join(dept, "deptno").select("ename", "mgr", "sal", "comm", "dname", "img");

            //Modify column names
            emp_dept = emp_dept.withColumnRenamed("ename", "Employee")
                    .withColumnRenamed("mgr", "Manager")
                    .withColumnRenamed("sal", "Salary")
                    .withColumnRenamed("comm", "Commission")
                    .withColumnRenamed("dname", "Department")
                    .withColumnRenamed("img", "Image");
            emp_dept.show();

            Iterable<Employee> elist = emp_dept.toJavaRDD().map((Row v1) -> {
                Employee employee = new Employee();
                employee.setName(v1.getString(0));
                employee.setManager(v1.getString(1));
                employee.setSalary(v1.getInt(2));
                employee.setCommission(v1.getInt(3));
                employee.setDepartmentName(v1.getString(4));
                employee.setImage(v1.getString(5));
                return employee;
            }).collect();


            model.addAttribute("elist", elist);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return "/display/index";
    }
}
