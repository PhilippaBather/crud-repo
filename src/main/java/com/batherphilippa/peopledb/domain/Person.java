package com.batherphilippa.peopledb.domain;

import com.batherphilippa.peopledb.annotation.Id;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class Person {
    @Id
    private Long id;
    private String firstname;
    private String lastname;
    private ZonedDateTime dob;
    private BigDecimal salary = new BigDecimal("0");
    private String email;
    private Optional<Address> homeAddress = Optional.empty();
    private Optional<Address> businessAddress = Optional.empty();
    private Optional<Person> spouse = Optional.empty();
    private Optional<Person> parent = Optional.empty();
    private Set<Person> children = new HashSet<>() {};

    public Person(String firstname, String lastname, ZonedDateTime dob) {
        this.firstname = firstname;
        this.lastname = lastname;
        this.dob = dob;
    }

    public Person(Long id, String firstname, String lastname, ZonedDateTime dob) {
        this(firstname, lastname, dob);
        this.id = id;
    }

    public Person(long personId, String firstName, String lastName, ZonedDateTime dob, BigDecimal salary) {
        this(personId, firstName,lastName, dob);
        this.salary = salary;
    }

    public Long getId() {
        return id;
    }

    public String getFirstname() {
        return firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public ZonedDateTime getDob() {
        return dob;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public Optional<Address> getHomeAddress() {
        return homeAddress;
    }
    public void setHomeAddress(Address homeAddress) {
        this.homeAddress = Optional.ofNullable(homeAddress);
    }

    public Optional<Address> getBusinessAddress() {
        return businessAddress;
    }

    public void setBusinessAddress(Address businessAddress) {
        this.businessAddress = Optional.ofNullable(businessAddress);
    }

    public Optional<Person> getSpouse() {
        return spouse;
    }

    public void setSpouse(Person spouse) {
        this.spouse = Optional.ofNullable(spouse);
    }


    public void addChild(Person child) {
            children.add(child);
            child.setParent(this);
    }

    public Optional<Person> getParent() {
        return parent;
    }

    private void setParent(Person parent) {
        this.parent = Optional.ofNullable(parent);
    }

    public Set<Person> getChildren() {
        return children;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(id, person.id) && Objects.equals(firstname, person.firstname) && Objects.equals(lastname, person.lastname) && Objects.equals(dob.withZoneSameInstant(ZoneId.of("+0")), person.dob.withZoneSameInstant(ZoneId.of("+0")));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstname, lastname, dob);
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", firstname='" + firstname + '\'' +
                ", lastname='" + lastname + '\'' +
                ", dob=" + dob +
                ", salary=" + salary +
                '}';
    }

}
