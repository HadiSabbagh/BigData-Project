package org.bigData.Entities;

import lombok.Data;

import java.io.Serializable;

@Data
public class Employee implements Serializable {
    Integer EmployeeId;
    String Name;
    String Job;
    Integer Manager;
    Integer Salary;
    Integer Commission;
    Integer Department;
    String Image;
}
