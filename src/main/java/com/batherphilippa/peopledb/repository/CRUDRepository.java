package com.batherphilippa.peopledb.repository;

import com.batherphilippa.peopledb.annotation.Id;
import com.batherphilippa.peopledb.annotation.MultiSQL;
import com.batherphilippa.peopledb.annotation.SQL;
import com.batherphilippa.peopledb.domain.CrudOperation;
import com.batherphilippa.peopledb.exception.DataException;
import com.batherphilippa.peopledb.exception.NoIdFoundException;
import com.batherphilippa.peopledb.exception.UnableToSetIdFieldException;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class CRUDRepository<T> {

    // protected to be seen by subclasses
    protected Connection connection;
    private PreparedStatement savedPS;
    private PreparedStatement findByIdPS;

    public CRUDRepository(Connection connection) throws DataException{
        try {
            this.connection = connection;
            this.savedPS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            this.findByIdPS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ONE, this::getFindByIdSql));
        } catch(SQLException e) {
            throw new DataException("Unable to create Prepared Statements for CRUDRepository", e);
        }
    }
    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> sqlStream = getSqlStream();
        Stream<SQL> multiSqlStream = getMultiSqlStream();
        return Stream.concat(sqlStream, multiSqlStream)
               // filter for annotations of the required operation type
                .filter(a -> a.operationType().equals(operationType))
               // then get it's value(s)
                .map(SQL::value)
                .findFirst()
               // passing ref to method; rather than calling it
                .orElseGet(sqlGetter);
    }

    private Stream<SQL> getSqlStream() {
        return Arrays.stream(this.getClass().getDeclaredMethods())
                // filter methods for those with SQL annotation defined on them
                .filter(m -> m.isAnnotationPresent(SQL.class))
                // convert to a stream of annotations
                .map(m -> m.getAnnotation(SQL.class));
    }

    private Stream<SQL> getMultiSqlStream() {
        return Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                // returns a stream of SQL
                .flatMap(msql -> Arrays.stream(msql.value()));
    }

    private Long getIdByAnnotation(T entity) {
        // convert array of fields into stream of fields
        return Arrays.stream(entity.getClass().getDeclaredFields())
                // find @Id annotation
                .filter(f -> f.isAnnotationPresent(Id.class))
                // stream of longs
                .map(f -> {
                    // allows us to tell Java to override field's access modifier
                    f.setAccessible(true);
                    // f is a field fo the class (not the object); the entity is the object
                    long id;
                    try {
                        id = (long) f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                }).findFirst()
                .orElseThrow(() -> new NoIdFoundException("No ID annotated field found."));
    }

    private void setIdByAnnotation(Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new UnableToSetIdFieldException("Unable to set ID field exception.");
                    }
                });
    }

    protected T save(T entity) {
        try {
            mapForSave(entity, savedPS);
            savedPS.executeUpdate();
            ResultSet resultSet = savedPS.getGeneratedKeys();
            while(resultSet.next()) {
                long id = resultSet.getLong(1);
                setIdByAnnotation(id, entity);
                postSave(id, entity);
                return entity;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    protected Optional<T> findById(Long id) {
        T entity = null;
        try {
            findByIdPS.setLong(1, id);
            ResultSet rs = findByIdPS.executeQuery();
            while(rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return Optional.ofNullable(entity);
    }

    protected List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement ps = connection.prepareStatement(
                    getSqlByAnnotation(CrudOperation.FIND_MANY, this::getFindAllSql),
                    // allows us to scroll; insensitive: get snapshot of a static dataset (so not affected by changes
                    // made by others connecting simultaneously
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    // read only; no updates to be made
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                entities.add(extractEntityFromResultSet(resultSet));
            }
        } catch(SQLException e){
            e.printStackTrace();
        }
        return entities;
    }

    protected long getCount() {
        long count = 0L;
        try {
            PreparedStatement prepStat = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountSql));
            ResultSet resultSet = prepStat.executeQuery();
            while(resultSet.next()) {
                count = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    protected void deleteById(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSql));
            ps.setLong(1, getIdByAnnotation(entity));
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // use var args to accept an array of an unspecified number of Person objects
    @SafeVarargs
    protected final void deleteById(T... entities) {
        try {
            // note: danger of SQL injection with this technique
            Statement statement = connection.createStatement();
            String ids = Arrays.stream(entities)
//                    .map(e -> findIdByAnnotation(e)) // or:
                    .map(this::getIdByAnnotation)
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));
            int affectedRecords = statement.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSql).replace(":ids", ids));
            System.out.println(affectedRecords);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected T updateById(T entity) {
        try {
            PreparedStatement prepStat = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateByIdSql));
            mapForUpdate(entity, prepStat);
            prepStat.setLong(1, getIdByAnnotation(entity));
            int rowsAffected = prepStat.executeUpdate();
            System.out.printf("updateId: rowsAffected: %d%n", rowsAffected);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    protected void postSave(long id, T entity) {}
    protected String getSaveSql() {throw new RuntimeException("SQL not defined");}
    protected String getUpdateByIdSql() { throw new RuntimeException("SQL not defined");}
    protected String getFindByIdSql() {throw new RuntimeException("SQL not defined");}
    protected String getFindAllSql() {throw new RuntimeException("SQL not defined");}
    protected String getCountSql() {throw new RuntimeException("SQL not defined");}
    protected String getDeleteSql() {throw new RuntimeException("SQL not defined");}
    protected String getDeleteInSql() {throw new RuntimeException("SQL not defined");}
    abstract PreparedStatement mapForUpdate(T entity, PreparedStatement prepStat) throws SQLException;
    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    /**
     * @param resultSet Result set
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;

}
