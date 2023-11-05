package org.bigData.Entities;

import lombok.Data;

import java.io.Serializable;
import java.util.Optional;

@Data
public class Employee implements Serializable {
    String Name;
    String Manager;
    Integer Salary;
    Integer Commission;
    String DepartmentName;
    String Image;
}
