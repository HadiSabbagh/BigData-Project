package org.bigData.Entities;

import lombok.Data;

import java.io.Serializable;
import java.util.Optional;

@Data
public class EmployeeDTO implements Serializable {
    Integer EmployeeId;
    String Name;
    String Job;
    String Manager;
    Integer Salary;
    Integer Commission;
    String DepartmentName;
    String Image;
}
