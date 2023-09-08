package com.batherphilippa.peopledb.repository;

import com.batherphilippa.peopledb.annotation.SQL;
import com.batherphilippa.peopledb.domain.Address;
import com.batherphilippa.peopledb.domain.CrudOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CRUDRepository<Address>{
    private static final String INSERT_ADDRESS_SQL = """
            INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS_2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY) VALUES(?,?,?,?,?,?,?,?);
            """;
    private static final String FIND_BY_ID_SQL = """
            SELECT ID, STREET_ADDRESS, ADDRESS_2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY FROM ADDRESSES WHERE ID=?;
            """;
    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    PreparedStatement mapForUpdate(Address address, PreparedStatement prepStat) throws SQLException {
        return null;
    }

    @Override
    @SQL(value = INSERT_ADDRESS_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Address address, PreparedStatement ps) throws SQLException {
        ps.setString(1, address.streetAddress());
        ps.setString(2, address.address2());
        ps.setString(3, address.city());
        ps.setString(4, address.state());
        ps.setString(5, address.postcode());
        ps.setString(6, address.county());
        ps.setString(7, address.region().toString());
        ps.setString(8, address.country());
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_ONE)
    Address extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
//        long addressId = resultSet.getLong("ID");
//        String streetAddress = resultSet.getString("STREET_ADDRESS");
//        String address2 = resultSet.getString("ADDRESS_2");
//        String city = resultSet.getString("CITY");
//        String state = resultSet.getString("STATE");
//        String postcode = resultSet.getString("POSTCODE");
//        String county = resultSet.getString("COUNTY");
//        // problematic if field is null
//        Region region = Region.valueOf(resultSet.getString("REGION").toUpperCase());
//        String country = resultSet.getString("COUNTRY");
//        return new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
        return null;
    }
}
