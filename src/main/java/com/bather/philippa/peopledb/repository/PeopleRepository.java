package com.bather.philippa.peopledb.repository;

import com.bather.philippa.peopledb.annotation.SQL;
import com.bather.philippa.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeopleRepository extends CRUDRepository<Person> {
    private static final String INSERT_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES(?, ?, ?)";
    private static final String FIND_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID=?";
    private static final String FIND_ALL_SQL = "SELECT * FROM PEOPLE";
    private static final String SELECT_COUNT_SQL = "SELECT COUNT(ID) AS COUNT FROM PEOPLE";
    private static final String DELETE_RECORD_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    private static final String DELETE_N_RECORDS_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_SALARY_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(INSERT_PERSON_SQL)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
    }

    @Override
    @SQL(UPDATE_SALARY_SQL)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException{
        long personId = rs.getLong("ID");  // or use column index
        String firstName = rs.getString("FIRST_NAME");
        String lastName = rs.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal("SALARY");
        return new Person(personId, firstName, lastName, dob, salary);
    }
    @Override
    String getFindByIdSql() { return FIND_BY_ID_SQL; }

    @Override
    String getFindAllSql() {
        return FIND_ALL_SQL;
    }

    @Override
    protected String getCountIdSql() {
        return SELECT_COUNT_SQL;
    }

    @Override
    protected String getDeleteRecordSql() {
        return DELETE_RECORD_SQL;
    }

    @Override
    protected String getDeleteNRecordsSql() {
        return DELETE_N_RECORDS_SQL;
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
