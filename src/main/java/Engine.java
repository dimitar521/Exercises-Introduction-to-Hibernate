import entities.Address;
import entities.Employee;
import entities.Project;
import entities.Town;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static org.hibernate.hql.internal.antlr.HqlTokenTypes.FROM;

public class Engine implements Runnable {
    private final EntityManager entityManager;
    private BufferedReader bufferedReader;

    public Engine(EntityManager entityManager) {
        this.entityManager = entityManager;
        this.bufferedReader = new BufferedReader(new InputStreamReader(System.in));
    }

    @Override
    public void run() {
        System.out.println("Please select exercise number! ");
        try {
            int exNum = Integer.parseInt(bufferedReader.readLine());
            switch (exNum) {
                case 1:
                    System.out.println("Already done! Next problem ex.2 Change Casing");

                case 2:
                    ChangeCasingExTwo();
                    System.out.println("Next problem ex.3 Contains Employee");
                case 3:
                    ContainsEmployeeEx3();
                    System.out.println("Next ex.4 Employees with Salary Over 50 000");
                case 4:
                    EmployeesWithSalaryOver50000ExFour();
                    System.out.println("Next ex.5 Employees from Department");
                case 5:
                    EmployeesFromDepartmentEx5();
                    System.out.println("Next ex.6 Adding a New Address and Updating Employee");
                case 6:
                    AddingANewAddressAndUpdatingEmployeeEX6();
                    System.out.println("Next ex.7. Addresses with Employee Count");
                case 7:
                    AddresseesEmployeeCountEx7();
                    System.out.println("Next ex.8. Get Employee with Project");
                case 8:
                    GetEmployeeWithProjectEx8();
                    System.out.println("Next ex.9 Find Latest 10 Projects");
                case 9:
                    FindLatest10ProjectsEx9();
                    System.out.println("Next ex.10 Increase Salaries");
                case 10:
                    IncreaseSalariesEx10();
                    System.out.println("Next ex.11 Find Employees by First Name");
                case 11:
                    FindEmployeesByFirstNameEx11();
                    System.out.println("Next ex.12 Employees Maximum Salaries");
                case 12:
                    EmployeesMaximumSalariesEx12();
                    System.out.println("Next ex.13 Remove Towns");
                case 13:
                    RemoveTownsEx13();



            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            entityManager.close();
        }

    }

    private void RemoveTownsEx13() throws IOException {
        System.out.println("Please enter town name :");
        String townName = bufferedReader.readLine();
        Town town;
        try {
            town = this.getTownByName(townName);
        } catch (NoResultException nre) {
            System.out.println("No town found!");
            return;
        }

        List<Employee> employees = this.getEmployeesByTownName(town.getName());
        List<Address> addresses = this.getAddressesByTownName(town.getName());

        this.entityManager.getTransaction().begin();

        this.setTownToNull(addresses);
        this.setAddressesToNull(employees);
        this.deleteNulledAddresses(addresses);
        this.entityManager.remove(town);

        this.entityManager.getTransaction().commit();

        System.out.printf("%d address in %s deleted", addresses.size(), townName);

    }

    private void deleteNulledAddresses(List<Address> addresses) {
        for (Address address : addresses) {
            this.entityManager.remove(this.entityManager.contains(address)
                    ? address
                    : this.entityManager.merge(address));
        }
    }

    private void setTownToNull(List<Address> addresses) {
        for (Address address : addresses) {
            this.entityManager.detach(address);
            address.setTown(null);
            this.entityManager.merge(address);
        }
    }

    private void setAddressesToNull(List<Employee> employees) {
        for (Employee employee : employees) {
            this.entityManager.detach(employee);
            employee.setAddress(null);
            this.entityManager.merge(employee);
        }
    }

    private List<Address> getAddressesByTownName(String townName) {
        return this.entityManager.createQuery(
                "SELECT a FROM Address a " +
                        "JOIN a.town t " +
                        "WHERE t.name LIKE :name ", Address.class)
                .setParameter("name", townName)
                .getResultList();
    }

    private List<Employee> getEmployeesByTownName(String name) {
        return this.entityManager.createQuery(
                "SELECT e FROM Employee e " +
                        "JOIN e.address a " +
                        "JOIN a.town t " +
                        "WHERE t.name LIKE :name ", Employee.class)
                .setParameter("name", name)
                .getResultList();
    }

    private Town getTownByName(String townName) {
        return this.entityManager.createQuery(
                "SELECT t FROM Town t " +
                        "WHERE t.name LIKE :townName", Town.class)
                .setParameter("townName", townName)
                .getSingleResult();
    }

    private void EmployeesMaximumSalariesEx12() {
        this.entityManager.createQuery(
                "SELECT emp FROM Employee emp " +
                        "WHERE emp.salary IN (SELECT MAX(e.salary) " +
                        "               FROM Employee e " +
                        "               JOIN e.department d " +
                        "               GROUP BY d.name " +
                        "               HAVING MAX(e.salary) NOT BETWEEN 30000 AND 70000) " +
                        "GROUP BY emp.department", Employee.class)
                .getResultStream()
                .forEach(e -> System.out.printf("%s %.2f%n",
                        e.getDepartment().getName(),
                        e.getSalary()));
    }

    private void FindEmployeesByFirstNameEx11() throws IOException {
        System.out.println("Enter a pattern: ");
        String pattern =bufferedReader.readLine()+"%";
        entityManager.createQuery("select  e from Employee e where e.firstName like :pattern",Employee.class)
                .setParameter("pattern",pattern).getResultStream().forEach(employee -> System.out.printf("%s %s - %s - ($%.2f)%n", employee.getFirstName(), employee.getLastName(), employee.getJobTitle(), employee.getSalary()));
    }

    private void IncreaseSalariesEx10() {
        entityManager.getTransaction().begin();
        int ids = entityManager.createQuery("update Employee e" +
                " set e.salary=e.salary*1.12" +
                " where e.department.id IN :ids").setParameter("ids", Set.of(1,2,4,11)).executeUpdate();
        entityManager.getTransaction().commit();
        System.out.println(ids);
    }

    private void FindLatest10ProjectsEx9() {
        entityManager.createQuery("select p from Project p " +
                "order by p.name ",Project.class).setMaxResults(10).getResultStream().sorted(Comparator.comparing(Project::getName)).forEach(project -> System.out.printf("Project name: %s%n" +
                " \tProject Description: %s%n" +
                " \tProject Start Date:%s%n" +
                " \tProject End Date: %s%n",project.getName(),project.getDescription(),project.getStartDate(),project.getEndDate()));
    }

    private void GetEmployeeWithProjectEx8() throws IOException {
        System.out.println("Enter Employee's ID: ");
        Integer id = Integer.parseInt(bufferedReader.readLine());

        Employee e = this.entityManager.find(Employee.class, id);
        System.out.printf("%s %s - %s%n",
                e.getFirstName(),
                e.getLastName(),
                e.getJobTitle());
        e.getProjects().stream()
                .sorted(Comparator.comparing(Project::getName))
                .forEach(project -> System.out.printf("\t%s%n", project.getName()));
    }
    private void AddresseesEmployeeCountEx7() {
        entityManager.createQuery("select a from Address a " +
                "order by a.employees.size desc ", Address.class).setMaxResults(10).getResultStream().forEach(address -> System.out.printf("%s, %s - %d employees%n",
                address.getText(), address.getTown()==null?"unknown":address.getTown().getName(), address.getEmployees()==null?"Unknown":address.getEmployees().size()));
    }

    private void AddingANewAddressAndUpdatingEmployeeEX6() throws IOException {
        System.out.println("Enter employee last name:");
        String lastName= bufferedReader.readLine();
        Employee l_name = entityManager.createQuery("Select e from Employee e " +
                " where e.lastName = :l_name", Employee.class).setParameter("l_name", lastName)
                .getSingleResult();

        Address address=createAddress("Vitoshka 15");
        entityManager.getTransaction().begin();
        l_name.setAddress(address);
        entityManager.getTransaction().commit();

    }

    private Address createAddress(String s) {
        Address address =new Address();
        address.setText(s);
        entityManager.getTransaction().begin();
        entityManager.persist(address);
        entityManager.getTransaction().commit();
        return address;
    }


    private void EmployeesFromDepartmentEx5() {
        entityManager.createQuery("select e from Employee e where e.department.name= :d_name order by e.salary,e.id",Employee.class).setParameter("d_name","Research and Development" ).getResultStream().forEach(employee -> {
            System.out.printf("%s %s from %s - $%.2f%n",employee.getFirstName(),employee.getLastName(),employee.getDepartment().getName(),employee.getSalary());
        });
    }

    private void EmployeesWithSalaryOver50000ExFour() {
         entityManager.createQuery("select e from Employee e where e.salary > :min_salary", Employee.class).setParameter("min_salary", BigDecimal.valueOf(50000L)).getResultStream().map(Employee::getFirstName)
         .forEach(System.out::println);

    }

    private void ContainsEmployeeEx3() throws IOException {
        System.out.println("Please enter employee full name:");
        String []fullName=bufferedReader.readLine().split("\\s+");
        String firstname=fullName[0];
        String lastName=fullName[1];
        Long singleResult = entityManager.createQuery("select count(e) from Employee e where e.firstName= :f_name and e.lastName= :l_name", Long.class).setParameter("f_name", firstname).setParameter("l_name", lastName).getSingleResult();
        System.out.println(singleResult==0?"No":"Yes");

    }

    private void ChangeCasingExTwo() {
        entityManager.getTransaction().begin();
        int i = entityManager.createQuery("UPDATE Town t " + "set t.name = upper(t.name) " + "where length(t.name)<=5 ").executeUpdate();
        System.out.println(i);
        entityManager.getTransaction().commit();
    }
}
