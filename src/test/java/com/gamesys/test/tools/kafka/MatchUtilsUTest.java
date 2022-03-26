package com.gamesys.test.tools.kafka;

import com.gamesys.test.tools.utils.TestData;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MatchUtilsUTest {

    private static final String LS = System.lineSeparator();

    @Test
    public void testFindValueBySimpleKeyNullKey() {
        String key = null;
        assertThat(MatchUtils.findValueByKey(TestData.generateTestRecordJson("x"), key), is(nullValue()));
    }

    @Test
    public void testFindValueBySimpleKeyNullJson() {
        String key = "firstName";
        assertThat(MatchUtils.findValueByKey(null, key), is(nullValue()));
    }

    @Test
    public void testFindValueBySimpleKeyInvalidJson() {
        String key = "firstName";
        assertThat(MatchUtils.findValueByKey("i_am_not_json}{", key), is(nullValue()));
    }

    @Test
    public void testFindValueBySimpleKeySuccess() {
        String key = "firstName";
        assertThat(MatchUtils.findValueByKey(TestData.generateTestRecordJson("x"), key), is("Joe-x"));
    }

    @Test
    public void testFindValueBySimpleKeyInvalidKey() {
        String key = "i_do_not_exist";
        assertThat(MatchUtils.findValueByKey(TestData.generateTestRecordJson("x"), key), is(nullValue()));
    }

    @Test
    public void testFindValueByCompoundKeySuccess() {
        String key = "address1.street";
        assertThat(MatchUtils.findValueByKey(TestData.generateTestRecordJson("x"), key), is("road1-x"));
    }

    @Test
    public void testFindValueByCompoundKeyInvalidKeys() {
        String key = "i_do_not_exist.street";
        assertThat(MatchUtils.findValueByKey(TestData.generateTestRecordJson("x"), key), is(nullValue()));
        key = "address1.i_do_not_exist";
        assertThat(MatchUtils.findValueByKey(TestData.generateTestRecordJson("x"), key), is(nullValue()));
    }

    @Test
    public void testQuerySuccess() {
        Map<String, String> query = new HashMap<>();
        query.put("firstName", "Joe-x");
        query.put("lastName", "Bloggs-x");
        query.put("address1.street", "road1-x");
        assertTrue(MatchUtils.match(TestData.generateTestRecordJson("x"), query));
    }

    @Test
    public void testQueryNotfound() {
        Map<String, String> query = new HashMap<>();
        query.put("firstName", "Joe-x");
        query.put("lastName", "Biggs-x");
        assertFalse(MatchUtils.match(TestData.generateTestRecordJson("x"), query));
    }

    @Test
    public void testQueryInvalidKey() {
        Map<String, String> query = new HashMap<>();
        query.put("firstName", "Joe-x");
        query.put("i_do_not_exist", "Bloggs-x");
        assertFalse(MatchUtils.match(TestData.generateTestRecordJson("x"), query));
    }
}