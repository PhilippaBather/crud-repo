package com.bather.philippa.peopledb.repository;

import com.bather.philippa.peopledb.annotation.SQL;
import com.bather.philippa.peopledb.model.Address;
import com.bather.philippa.peopledb.model.CrudOperation;
import com.bather.philippa.peopledb.model.Person;
import com.bather.philippa.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

public class PersonRepository extends CrudRepository<Person> {
    private AddressRepository addressRepository = null;
    private static final String INSERT_PERSON_SQL = """
            INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, SPOUSE, PARENT_ID)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    private static final String FIND_BY_ID_SQL = """
            SELECT
            PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL,
            CHILD.ID AS CHILD_ID, CHILD.FIRST_NAME AS CHILD_FIRST_NAME, CHILD.LAST_NAME AS CHILD_LAST_NAME, CHILD.DOB AS CHILD_DOB, CHILD.SALARY AS CHILD_SALARY, CHILD.EMAIL AS CHILD_EMAIL,
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY,
            HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
            BUSINESS.ID AS BUSINESS_ID, BUSINESS.STREET_ADDRESS AS BUSINESS_STREET_ADDRESS, BUSINESS.ADDRESS2 AS BUSINESS_ADDRESS2, BUSINESS.CITY AS BUSINESS_CITY,
            BUSINESS.STATE AS BUSINESS_STATE, BUSINESS.POSTCODE AS BUSINESS_POSTCODE, BUSINESS.COUNTY AS BUSINESS_COUNTY, BUSINESS.REGION AS BUSINESS_REGION, BUSINESS.COUNTRY AS BUSINESS_COUNTRY
            FROM PEOPLE AS PARENT
            LEFT OUTER JOIN PEOPLE AS CHILD ON PARENT.ID=CHILD.PARENT_ID
            LEFT OUTER JOIN ADDRESSES AS HOME ON PARENT.HOME_ADDRESS=HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BUSINESS ON PARENT.BUSINESS_ADDRESS=BUSINESS.ID
            WHERE PARENT.ID=?""";
    private static final String FIND_ALL_SQL = "SELECT * FROM PEOPLE";
    private static final String SELECT_COUNT_SQL = "SELECT COUNT(ID) AS COUNT FROM PEOPLE";
    private static final String DELETE_RECORD_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    private static final String DELETE_N_RECORDS_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_SALARY_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PersonRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }

    @Override
    @SQL(value = INSERT_PERSON_SQL,operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        associateAddressWithPerson(ps, entity.getHomeAddress(), 6);
        associateAddressWithPerson(ps, entity.getBusinessAddress(), 7);
        associateSpouseWithPerson(ps, entity.getSpouse(), 8);
        associateChildWithPerson(entity, ps);
    }

    private static void associateChildWithPerson(Person entity, PreparedStatement ps) throws SQLException {
        Optional<Person> parent = entity.getParent();
        if (parent.isPresent()) {
            ps.setLong(9, parent.get().getId());
        } else {
            ps.setObject(9, null);
        }
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().stream()
                .forEach(this::save);
    }

    private void associateSpouseWithPerson(PreparedStatement ps, Optional<Person> spouse, int paramIndex) throws SQLException {
        if (spouse.isPresent()) {
            Person savedSpouse = save(spouse.get());
            ps.setLong(paramIndex, savedSpouse.getId());
        } else {
            ps.setObject(paramIndex, null);
        }
    }

    private void associateAddressWithPerson(PreparedStatement ps, Optional<Address> address, int paramIndex) throws SQLException {
        if (address.isPresent()) {
            Address savedAddress = addressRepository.save(address.get());
            ps.setLong(paramIndex, savedAddress.id());
        } else {
            ps.setObject(paramIndex, null);
        }
    }

    @Override
    @SQL(value = UPDATE_SALARY_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_RECORD_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_N_RECORDS_SQL, operationType = CrudOperation.DELETE_MANY)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException{
        Person parent = null;

        do {
            Person curParent = extractPerson(rs, "PARENT_");
            if (parent == null) {
                parent = curParent;
            } else if (!parent.equals(curParent)) {
                // TODO
            }

            Person child = extractPerson(rs, "CHILD_");

            Address homeAddress = extractAddress(rs, "HOME_");
            Address businessAddress = extractAddress(rs, "BUSINESS_");

            parent.setHomeAddress(homeAddress);
            parent.setBusinessAddress(businessAddress);
            parent.addChild(child);

        } while (rs.next());
        return parent;
    }

    private Person extractPerson(ResultSet rs, String aliasPrefix) throws SQLException {
        long personId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);  // or use column index
        String firstName = getValueByAlias(aliasPrefix + "FIRST_NAME", rs, String.class);
        String lastName = getValueByAlias(aliasPrefix + "LAST_NAME", rs, String.class);
        ZonedDateTime dob = ZonedDateTime.of(getValueByAlias(aliasPrefix + "DOB", rs, Timestamp.class).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = getValueByAlias(aliasPrefix + "SALARY", rs, BigDecimal.class);
        Person person = new Person(personId, firstName, lastName, dob, salary);
        return person;
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
        Long addressId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);  // see utility method for other means of extracting alias
        if (addressId == null) return null;
        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", rs, String.class);
        String address2 = getValueByAlias(aliasPrefix + "ADDRESS2", rs, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", rs, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", rs, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", rs, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", rs, String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", rs, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", rs, String.class);
        Address address = new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
        return address;
    }

    private <T> T getValueByAlias(String alias, ResultSet rs, Class<T> clazz) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int colIdx = 1; colIdx <= columnCount; colIdx++) {
            if (alias.equals(rs.getMetaData().getColumnLabel(colIdx))) {
                return (T) rs.getObject(colIdx);
            }
        }
        throw new SQLException(String.format("Column not found for alias: '%s'.%n", alias));
    }

    private Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
