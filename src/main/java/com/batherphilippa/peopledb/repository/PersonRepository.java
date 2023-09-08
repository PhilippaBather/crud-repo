package com.batherphilippa.peopledb.repository;

import com.batherphilippa.peopledb.annotation.SQL;
import com.batherphilippa.peopledb.domain.Address;
import com.batherphilippa.peopledb.domain.CrudOperation;
import com.batherphilippa.peopledb.domain.Person;
import com.batherphilippa.peopledb.domain.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class PersonRepository extends CRUDRepository<Person> {
    private final AddressRepository addressRepo;
    private final Map<String, Integer> aliasColumnIdxMap = new HashMap<>();
    private static final String INSERT_PERSON_SQL = """
            INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, EMAIL, SALARY, HOME_ADDRESS, BUSINESS_ADDRESS, SPOUSE, PARENT_ID) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
            """;
    private static final String FIND_PERSON_BY_ID_SQL = """
            SELECT
            P.ID AS P_ID, P.FIRST_NAME AS P_FIRST_NAME, P.LAST_NAME AS P_LAST_NAME, P.DOB AS P_DOB, P.SALARY AS P_SALARY, P.HOME_ADDRESS AS P_HOME_ADDRESS,
            P.BUSINESS_ADDRESS AS P_BUSINESS_ADDRESS, P.SPOUSE AS P_SPOUSE, P.PARENT_ID AS P_PARENT_ID,
            S.ID AS S_ID, S.FIRST_NAME AS S_FIRST_NAME, S.LAST_NAME AS S_LAST_NAME, S.DOB AS S_DOB, S.SALARY AS S_SALARY, S.HOME_ADDRESS AS S_HOME_ADDRESS,
            S.BUSINESS_ADDRESS AS S_BUSINESS_ADDRESS, S.SPOUSE AS S_SPOUSE, S.PARENT_ID AS S_PARENT_ID,
            C.ID AS C_ID, C.FIRST_NAME AS C_FIRST_NAME, C.LAST_NAME AS C_LAST_NAME, C.DOB AS C_DOB, C.SALARY AS C_SALARY, C.HOME_ADDRESS AS C_HOME_ADDRESS,
            C.BUSINESS_ADDRESS AS C_BUSINESS_ADDRESS, C.SPOUSE AS C_SPOUSE, C.PARENT_ID AS C_PARENT_ID,
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS_2 AS HOME_ADDRESS_2, HOME.CITY AS HOME_CITY,
             HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
            BUSINESS.ID AS BUSINESS_ID, BUSINESS.STREET_ADDRESS AS BUSINESS_STREET_ADDRESS, BUSINESS.ADDRESS_2 AS BUSINESS_ADDRESS_2, BUSINESS.CITY AS BUSINESS_CITY,
             BUSINESS.STATE AS BUSINESS_STATE, BUSINESS.POSTCODE AS BUSINESS_POSTCODE, BUSINESS.COUNTY AS BUSINESS_COUNTY, BUSINESS.REGION AS BUSINESS_REGION,
             BUSINESS.COUNTRY AS BUSINESS_COUNTRY
            FROM PEOPLE AS P
            LEFT OUTER JOIN ADDRESSES AS HOME
            ON P.HOME_ADDRESS=HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BUSINESS
            ON P.BUSINESS_ADDRESS=BUSINESS.ID
            LEFT OUTER JOIN PEOPLE AS S
            ON P.SPOUSE=S.ID
            LEFT OUTER JOIN PEOPLE AS C
            ON P.ID=C.PARENT_ID
            WHERE P.ID=?;
            """;
    private static final String FIND_ALL_SQL = """
            SELECT
            P.ID AS P_ID, P.FIRST_NAME AS P_FIRST_NAME, P.LAST_NAME AS P_LAST_NAME, P.DOB AS P_DOB, P.SALARY AS P_SALARY,
            P.HOME_ADDRESS AS P_HOME_ADDRESS, P.BUSINESS_ADDRESS AS P_BUSINESS_ADDRESS, P.SPOUSE AS P_SPOUSE,
            P.PARENT_ID AS P_PARENT_ID,
            FROM PEOPLE AS P
            FETCH FIRST 20 ROWS ONLY;
            """;
    private static final String GET_COUNT_SQL = """
            SELECT COUNT(ID) FROM PEOPLE;
            """;
    private static final String DELETE_PERSON_BY_ID_SQL = """
            DELETE FROM PEOPLE WHERE ID=?;
            """;
    private static final String DELETE_PEOPLE_BY_ID_SQL = """
            DELETE FROM PEOPLE WHERE ID IN (:ids);
            """;
    private static final String UPDATE_PERSON_BY_ID_SQL = """
            UPDATE PEOPLE SET ID=?, FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?;
            """;
    public PersonRepository(Connection connection) {
        super(connection);
        this.addressRepo = new AddressRepository(connection);
    }
    private Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
    @Override
    @SQL(value=FIND_PERSON_BY_ID_SQL, operationType= CrudOperation.FIND_ONE)
    @SQL(value=FIND_ALL_SQL, operationType= CrudOperation.FIND_MANY)
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
        Person finalParent = null;
        do {
            // .get() as if no record, the method should not return anything
            Person currentParent = extractPerson(resultSet, "P_").get();
            if (finalParent == null) {
                finalParent = currentParent;
            }
            if(!finalParent.equals(currentParent)) {
                resultSet.previous();
                break;
            }
            Address homeAddress = extractAddress(resultSet, "HOME_");
            // extract business address
            Address businessAddress = extractAddress(resultSet, "BUSINESS_");
            // extract spouse
            Optional<Person> spouse = extractPerson(resultSet, "S_");
            // extract child
            Optional<Person> child = extractPerson(resultSet, "C_");
            // takes a regular Address, but internally wraps it within an Optional
            finalParent.setHomeAddress(homeAddress);
            finalParent.setBusinessAddress(businessAddress);
            spouse.ifPresent(finalParent::setSpouse);
            child.ifPresent(finalParent::addChild);
        } while (resultSet.next());
        return finalParent;
    }

    private Optional<Person> extractPerson(ResultSet resultSet, String aliasPrefix) throws SQLException {
        Long personId = getValueByAlias(aliasPrefix.concat("ID"), resultSet, Long.class);
        if (personId == null) {return Optional.empty();}
        String firstName = getValueByAlias(aliasPrefix.concat("FIRST_NAME"), resultSet, String.class);
        String lastName = getValueByAlias(aliasPrefix.concat("LAST_NAME"), resultSet, String.class);
        ZonedDateTime dob = ZonedDateTime.of(getValueByAlias(aliasPrefix.concat("DOB"), resultSet, Timestamp.class).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = getValueByAlias(aliasPrefix.concat("SALARY"), resultSet, BigDecimal.class);
        Person person = new Person(personId, firstName, lastName, dob, salary);
        return Optional.of(person);
    }

    private Address extractAddress(ResultSet resultSet, String aliasPrefix) throws SQLException {
        // alternative means of retrieving addressId if column aliases not picked up automatically
        Long addressId = getValueByAlias(aliasPrefix.concat("ID"), resultSet, Long.class);
        if (addressId == null) {return null;}
        // easy means: if column aliases picked up automatically, simply pass the alias as a param; but less portable
        // as depends on all DB having this automatic functionality
//        long addressId = resultSet.getLong("A_ID");

        String streetAddress =  getValueByAlias(aliasPrefix.concat("STREET_ADDRESS"), resultSet, String.class);
        String address2 =  getValueByAlias(aliasPrefix.concat("ADDRESS_2"), resultSet, String.class);
        String city =  getValueByAlias(aliasPrefix.concat("CITY"), resultSet, String.class);
        String state =  getValueByAlias(aliasPrefix.concat("STATE"), resultSet, String.class);
        String postcode =  getValueByAlias(aliasPrefix.concat("POSTCODE"), resultSet, String.class);
        String county =  getValueByAlias(aliasPrefix.concat("COUNTY"), resultSet, String.class);
        // problematic if field is null
        Region region = Region.valueOf(getValueByAlias(aliasPrefix.concat("REGION"), resultSet, String.class).toUpperCase());
        String country =  getValueByAlias(aliasPrefix.concat("COUNTRY"), resultSet, String.class);
        return new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
    }
    private <T> T getValueByAlias(String alias, ResultSet resultSet, Class<T> clazz) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        int foundIndex = getAliasIndex(alias, resultSet, columnCount);
        return foundIndex == 0 ? null : (T) resultSet.getObject(foundIndex);
        // method doesn't have to return a value if throws an exception
        // in this case it could make more sense to indicate that the anticipated alias doesn't exist
//        throw new SQLException(String.format("Column not found for alias: %s", alias));
    }

    private Integer getAliasIndex(String alias, ResultSet resultSet, int columnCount) throws SQLException {
        Integer foundIndx = aliasColumnIdxMap.getOrDefault(alias, 0);
        if (foundIndx == 0) {
            for(int colIndex = 1; colIndex <= columnCount; colIndex++) {
                if(alias.equals(resultSet.getMetaData().getColumnLabel(colIndex))) {
                    foundIndx = colIndex;
                    // cache aliases
                    aliasColumnIdxMap.put(alias, foundIndx);
                    break;
    //                return resultSet.getObject(colIndex, clazz);
                    // alternatively cast return (T) resultSet.getObject(colIndex);
                }
            }
        }
        return foundIndx;
    }

    @Override
    @SQL(value=INSERT_PERSON_SQL, operationType= CrudOperation.SAVE)
    @SQL(value=GET_COUNT_SQL, operationType= CrudOperation.COUNT)
    @SQL(value=DELETE_PERSON_BY_ID_SQL, operationType= CrudOperation.DELETE_ONE)
    @SQL(value=DELETE_PEOPLE_BY_ID_SQL, operationType= CrudOperation.DELETE_MANY)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException{
        ps.setString(1, entity.getFirstname());
        ps.setString(2, entity.getLastname());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setString(4, entity.getEmail());
        ps.setBigDecimal(5, entity.getSalary());
        associateAddressWithPerson(ps, entity.getHomeAddress(), 6);
        associateAddressWithPerson(ps, entity.getBusinessAddress(), 7);
        associateSpouseWithPerson(ps, entity.getSpouse(), 8);
        associateChildWithPerson(ps, entity, 9);
    }

    private void associateChildWithPerson(PreparedStatement ps, Person entity, int paramIndex) throws SQLException {
        Optional<Person> parent = entity.getParent();
        if (parent.isPresent()) {
            ps.setLong(paramIndex, parent.get().getId());
        } else {
            ps.setObject(paramIndex, null);
        }
    }

    @Override
    protected void postSave(long id, Person entity) {
        entity.getChildren()
                .stream()
                .forEach(this::save);
    }

    private void associateAddressWithPerson(PreparedStatement ps, Optional<Address> address, int paramIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepo.save(address.get());
            ps.setLong(paramIndex, savedAddress.id());
        } else {
            ps.setObject(paramIndex, null);
        }
    }

    private void associateSpouseWithPerson(PreparedStatement ps, Optional<Person> spouse, int paramIndex) throws SQLException {
        Person savedSpouse;
        if (spouse.isPresent()) {
            savedSpouse = save(spouse.get());
            ps.setLong(paramIndex, savedSpouse.getId());
        } else {
            ps.setObject(paramIndex, null);
        }
    }

    @Override
    @SQL(value=UPDATE_PERSON_BY_ID_SQL, operationType= CrudOperation.UPDATE)
    PreparedStatement mapForUpdate(Person entity, PreparedStatement prepStat) throws SQLException {
        prepStat.setLong(1, entity.getId());
        prepStat.setString(2, entity.getFirstname());
        prepStat.setString(3, entity.getLastname());
        prepStat.setTimestamp(4, convertDobToTimestamp(entity.getDob()));
        prepStat.setBigDecimal(5, entity.getSalary());
        prepStat.setLong(6, entity.getId());
        return prepStat;
    }
}