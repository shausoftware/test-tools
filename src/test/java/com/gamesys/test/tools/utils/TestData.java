package com.gamesys.test.tools.utils;

public class TestData {

    private static final String LS = System.lineSeparator();

    public static String generateTestKeyJson(String id) {
        StringBuilder json = new StringBuilder();
        json.append("{").append(LS);
        json.append("  'key1': 'key1_value-").append(id).append("',").append(LS);
        json.append("  'key2': 'key2_value-").append(id).append("',").append(LS);
        json.append("  'key3': {").append(LS);
        json.append("    'keyPart': 'key3_part_value-").append(id).append("'").append(LS);
        json.append("  },").append(LS);
        json.append("  'key4': {").append(LS);
        json.append("    'keyPart': 'key4_part_value-").append(id).append("'").append(LS);
        json.append("  }").append(LS);
        json.append("}").append(LS);
        return json.toString();
    }

    public static String generateTestRecordJson(String id) {
        StringBuilder json = new StringBuilder();
        json.append("{").append(LS);
        json.append("  'firstName': 'Joe-").append(id).append("',").append(LS);
        json.append("  'lastName': 'Bloggs-").append(id).append("',").append(LS);
        json.append("  'address1': {").append(LS);
        json.append("    'street': 'road1-").append(id).append("'").append(LS);
        json.append("  },").append(LS);
        json.append("  'address2': {").append(LS);
        json.append("    'street': 'road2-").append(id).append("'").append(LS);
        json.append("  }").append(LS);
        json.append("}").append(LS);
        return json.toString();
    }
}
