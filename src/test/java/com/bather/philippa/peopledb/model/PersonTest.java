package com.bather.philippa.peopledb.model;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PersonTest {

    @Test
    public void testForEquality() {
        Person p1 = new Person("p1", "smith", ZonedDateTime.of(2000, 01, 1, 1, 01, 00, 0, ZoneId.of("+0")));
        Person p2 = new Person("p1", "smith", ZonedDateTime.of(2000, 01, 1, 1, 01, 00, 0, ZoneId.of("+0")));
        assertThat(p1).isEqualTo(p2);
    }

    @Test
    public void testForInEquality() {
        Person p1 = new Person("p1", "smith", ZonedDateTime.of(2000, 01, 1, 1, 01, 00, 0, ZoneId.of("+0")));
        Person p2 = new Person("p2", "smith", ZonedDateTime.of(2000, 01, 1, 1, 01, 00, 0, ZoneId.of("+0")));
        assertThat(p1).isNotEqualTo(p2);
    }
}