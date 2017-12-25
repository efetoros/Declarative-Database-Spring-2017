package db;
import java.util.LinkedHashMap;

public class Row {

    LinkedHashMap<String, String> columns;

    public Row() {
        columns = new LinkedHashMap<String, String>();
    }

    public void add(String key, String value) {
        columns.put(key, value);
    }
}