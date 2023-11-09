package org.bigData.Controllers;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.bigData.Entities.Employee;
import org.bigData.Entities.EmployeeDTO;
import org.bigData.Services.Reader.ReadCsvFileImpl;
import org.bigData.Services.Spark.SparkSessionStarter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Optional;

import static org.apache.spark.sql.functions.coalesce;
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
                    .add("mgr", DataTypes.IntegerType, true)
                    .add("hiredate", DataTypes.StringType, false)
                    .add("sal", DataTypes.IntegerType, false)
                    .add("comm", DataTypes.IntegerType, true)
                    .add("deptno", DataTypes.IntegerType, false)
                    .add("img", DataTypes.StringType, false);

            StructType dept_schema = new StructType()
                    .add("deptno", DataTypes.IntegerType, false)
                    .add("dname", DataTypes.StringType, false)
                    .add("loc", DataTypes.IntegerType, false);


            Dataset<Row> emp = readCsvFile.entity(session, "emp.csv", hdfsPath, emp_schema);
            emp.show();
            Dataset<Row> dept = readCsvFile.entity(session, "dept.csv", hdfsPath, dept_schema);

            //Self-join in employee's table to translate mgr id to ename
            Dataset<Row> df1 = emp.as("df1");
            Dataset<Row> df2 = emp.as("df2");
            emp = df1.join(df2, col("df1.empno").equalTo(df2.col("mgr")), "right")
                    .select(col("df2.empno"), col("df2.ename"), col("df1.ename").as("mgr"), col("df2.sal"),
                            col("df2.comm"), col("df2.deptno"), col("df2.img"));
            emp.show();
            //Join emp and dept
            Dataset<Row> emp_dept = emp.join(dept, "deptno").select("empno", "ename", "mgr", "sal", "comm", "dname", "img");

            //Modify column names
            emp_dept = emp_dept.withColumnRenamed("ename", "Employee")
                    .withColumnRenamed("empno", "EmployeeId")
                    .withColumnRenamed("mgr", "Manager")
                    .withColumnRenamed("sal", "Salary")
                    .withColumnRenamed("comm", "Commission")
                    .withColumnRenamed("dname", "Department")
                    .withColumnRenamed("img", "Image");
            emp_dept.show();

            Iterable<EmployeeDTO> elist = emp_dept.toJavaRDD().map((Row v1) -> {

                EmployeeDTO employeeDTO = new EmployeeDTO();
                employeeDTO.setEmployeeId(v1.getInt(0));
                employeeDTO.setName(v1.getString(1));
                employeeDTO.setManager(v1.getString(2));
                employeeDTO.setSalary(v1.getInt(3));
                if (!v1.isNullAt(4)) {
                    employeeDTO.setCommission(v1.getInt(4));
                }
                employeeDTO.setDepartmentName(v1.getString(5));
                employeeDTO.setImage(v1.getString(6));
                return employeeDTO;
            }).collect();


            model.addAttribute("elist", elist);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return "/display/index";
    }
}
