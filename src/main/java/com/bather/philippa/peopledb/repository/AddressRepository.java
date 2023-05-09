package com.bather.philippa.peopledb.repository;

import com.bather.philippa.peopledb.annotation.SQL;
import com.bather.philippa.peopledb.model.Address;
import com.bather.philippa.peopledb.model.CrudOperation;
import com.bather.philippa.peopledb.model.Region;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CrudRepository<Address> {

    private static final String INSERT_ADDRESS = """
            INSERT INTO ADDRESSES(STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY) 
            values(?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String FIND_BY_ID = """
            SELECT ID, STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY
            FROM ADDRESSES
            WHERE ID = ? 
            """;
    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement ps) throws SQLException {

    }

    @Override
    @SQL(value=INSERT_ADDRESS, operationType = CrudOperation.SAVE)
    void mapForSave(Address entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.streetAddress());
        ps.setString(2, entity.address2());
        ps.setString(3, entity.city());
        ps.setString(4, entity.state());
        ps.setString(5, entity.postcode());
        ps.setString(6, entity.county());
        ps.setString(7, entity.region().toString());
        ps.setString(8, entity.country());
    }

    @Override
    @SQL(value=FIND_BY_ID, operationType = CrudOperation.FIND_BY_ID)
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long id = rs.getLong("ID");
        String streetAddress = rs.getString("STREET_ADDRESS");
        String address2 = rs.getString("ADDRESS2");
        String city = rs.getString("CITY");
        String state = rs.getString("STATE");
        String postcode = rs.getString("POSTCODE");
        String county = rs.getString("COUNTY");
        Region region = Region.valueOf(rs.getString("REGION").toUpperCase());
        String country = rs.getString("Country");
        return new Address(id, streetAddress, address2, city, state, postcode, country, county, region);
    }
}
