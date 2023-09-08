package com.batherphilippa.peopledb.repository;

import com.batherphilippa.peopledb.domain.Address;
import com.batherphilippa.peopledb.domain.Person;
import com.batherphilippa.peopledb.domain.Region;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PersonRepositoryTests {

    private Connection connection;
    private PersonRepository peopleRepo;

    @BeforeEach
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:~/peopledb".replace("~", System.getProperty("user.home")));
        // turn off auto-commits to prevent test data contaminating DB
        connection.setAutoCommit(false);
        peopleRepo = new PersonRepository(connection);
    }

    // closes connection after every test
    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSavePerson() {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson = peopleRepo.save(person);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSavePeople() {
        Person person1 = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson1 = peopleRepo.save(person1);
        Person person2 = new Person("Jake", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson2 = peopleRepo.save(person2);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canSavePersonWithHomeAddress() {
        Address address = new Address(null, "34 Hawthorn Close", "null", "Chichester", "WS", "PO19 3DZ", "UK", "West Sussex", Region.SOUTH);
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.setHomeAddress(address);
        Person savedPerson = peopleRepo.save(person);
        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithBusinessAddress() {
        Address address = new Address(null, "34 Hawthorn Close", "null", "Chichester", "WS", "PO19 3DZ", "UK", "West Sussex", Region.SOUTH);
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.setBusinessAddress(address);
        Person savedPerson = peopleRepo.save(person);
        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSaveSpouse() throws SQLException {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person spouse = new Person("Jackie", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.setSpouse(spouse);
        Person savedPerson = peopleRepo.save(person);
        assertThat(savedPerson.getSpouse().get().getId()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithChildren() throws SQLException {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.addChild(new Person("Sammy", "Smith", ZonedDateTime.of(2006,10,10,15,3,0,0, ZoneId.of("-6"))));
        person.addChild(new Person("Lucy", "Smith", ZonedDateTime.of(2016,8,5,15,1,0,0, ZoneId.of("-6"))));
        person.addChild(new Person("George", "Smith", ZonedDateTime.of(2022,4,21,6,5,0,0, ZoneId.of("-6"))));
        Person savedPerson = peopleRepo.save(person);
        savedPerson.getChildren()
                .stream()
                .map(Person::getId)
                .forEach(id -> assertThat(id).isGreaterThan(0));
    }

    @Test
    public void canFindPersonById() {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson = peopleRepo.save(person);
        Person retrievedPerson = peopleRepo.findById(savedPerson.getId()).get();
        assertThat(retrievedPerson).isEqualTo(savedPerson);
    }

    @Test
    public void cannotFindPersonById() {
        Optional<Person> retrievedPerson = peopleRepo.findById(-1L);
        assertThat(retrievedPerson).isEmpty();
    }

    @Test
    public void canFindPersonByIdWithAddress() {
        Address address = new Address(null, "34 Hawthorn Close", "null", "Chichester", "WS", "PO19 3DZ", "UK", "West Sussex", Region.SOUTH);
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.setHomeAddress(address);
        Person savedPerson = peopleRepo.save(person);
        Person foundPerson = peopleRepo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getHomeAddress().get().county()).isEqualTo("West Sussex");
    }

    @Test
    public void canFindPersonByIdWithBusinessAddress() {
        Address address = new Address(null, "34 Hawthorn Close", "null", "Chichester", "WS", "PO19 3DZ", "UK", "West Sussex", Region.SOUTH);
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.setBusinessAddress(address);
        Person savedPerson = peopleRepo.save(person);
        Person foundPerson = peopleRepo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getBusinessAddress().get().county()).isEqualTo("West Sussex");
    }

    @Test
    public void canFindPersonByIdWithSpouse() {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person spouse = new Person("Jackie", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.setSpouse(spouse);
        Person savedPerson = peopleRepo.save(person);
        Person foundPerson = peopleRepo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getSpouse().get().getFirstname()).isEqualTo("Jackie");
    }

    @Test
    public void canFindPersonByIdWithChildren() {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        person.addChild(new Person("Sammy", "Smith", ZonedDateTime.of(2006,10,10,15,3,0,0, ZoneId.of("-6"))));
        person.addChild(new Person("Lucy", "Smith", ZonedDateTime.of(2016,8,5,15,1,0,0, ZoneId.of("-6"))));
        person.addChild(new Person("George", "Smith", ZonedDateTime.of(2022,4,21,6,5,0,0, ZoneId.of("-6"))));
        Person savedPerson = peopleRepo.save(person);
        Person foundPerson = peopleRepo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getChildren()
                .stream()
                .map(Person::getFirstname)
                .collect(Collectors.toSet())).contains("Sammy", "Lucy", "George");
    }


    @Test
    void canFindAllPeople() {
        long numRecords = peopleRepo.getCount();
        Person person1 = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        peopleRepo.save(person1);
        Person person2 = new Person("Jake", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        peopleRepo.save(person2);
        Person person3 = new Person("Jackson", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        peopleRepo.save(person3);
        List<Person> people = peopleRepo.findAll();
        assertThat((long) people.size()).isGreaterThanOrEqualTo(3);
    }

    @Test
    @Disabled
    public void canGetRecordCount() {
        long startCount = peopleRepo.getCount();
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        peopleRepo.save(person);
        long endCount = peopleRepo.getCount();
        assertThat(startCount).isEqualTo(endCount - 1);
    }

    @Test
    public void canDeletePersonById() {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson = peopleRepo.save(person);
        long startCount = peopleRepo.getCount();
        // get count of records
        peopleRepo.deleteById(savedPerson);
        long endCount = peopleRepo.getCount();
        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeletePeopleById() {
        Person person1 = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson1 = peopleRepo.save(person1);
        Person person2 = new Person("Jake", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson2 = peopleRepo.save(person2);
        long startCount = peopleRepo.getCount();
        peopleRepo.deleteById(savedPerson1, savedPerson2);
        long endCount = peopleRepo.getCount();
        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdatePersonById() {
        Person person = new Person("John", "Smith", ZonedDateTime.of(1980,11,15,15,15,0,0, ZoneId.of("-6")));
        Person savedPerson = peopleRepo.save(person);
        Person retrievedPerson = peopleRepo.findById(savedPerson.getId()).get();
        System.out.println(retrievedPerson);
        retrievedPerson.setSalary(new BigDecimal("1000.00"));
        peopleRepo.updateById(retrievedPerson);
        Person updatedPerson = peopleRepo.findById(retrievedPerson.getId()).get();
        System.out.println(savedPerson);
        System.out.println(updatedPerson);
        assertThat(savedPerson.getSalary()).isNotEqualByComparingTo(updatedPerson.getSalary());
    }

    @Test
    @Disabled
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("C://Users/bathe/downloads/Hr5m.csv"))
                .skip(1)
//                .limit(20)
                // convert into a stream of String arrays
                .map(line -> line.split(","))
                // convert into Person objects
                .map(strArr -> {
                    LocalDate dob = LocalDate.parse(strArr[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(strArr[11], DateTimeFormatter.ofPattern("hh:mm:ss a", Locale.US));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(strArr[2], strArr[4], zdtob);
                    person.setEmail(strArr[6]);
                    person.setSalary(new BigDecimal(strArr[25]));
                    return person;
                })
                .forEach(peopleRepo::save); // p -> personRepo.save(p)
        connection.commit();
    }

    @Test
    @Disabled
    public void experiment() {
        Person p1 = new Person(10L, null, null, null);
        Person p2 = new Person(20L, null, null, null);
        Person p3 = new Person(30L, null, null, null);
        Person p4 = new Person(40L, null, null, null);
        Person p5 = new Person(50L, null, null, null);
        // create Object[] array
        // transform this array to a specific Person[] array
        Person[] people = Arrays.asList(p1, p2, p3, p4, p5).toArray(new Person[]{});
        // use streams API to transform Person[] to stream of id strings delimited by commas
        String ids = Arrays.stream(people)
                .map(Person::getId)
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }

}
