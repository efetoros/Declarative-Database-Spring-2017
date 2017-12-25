//package db;
//
//import db.Database;
//import db.Row;
//
//import java.util.Iterator;
//
//public class Select {
//    private String print(String tablename) {
//        Database.Table table1 = tables.get(tablename);
//        Iterator columnNamesIterator = table1.columnNames.iterator();
//        String tableReturn = "";
//        while (columnNamesIterator.hasNext()) {
//            String colName = (String) columnNamesIterator.next();
//            tableReturn += colName;
//            if (columnNamesIterator.hasNext()) {
//                tableReturn += ",";
//            }
//        }
//
//        tableReturn += "\n";
//
//        for (Row row : table1.rows.values()) {
//            Iterator columnNameIterator = row.columns.keySet().iterator();
//            while (columnNameIterator.hasNext()) {
//                String colName = (String) columnNameIterator.next();
//                if (colName.split("\\s")[1].equals("string")) {
//                    tableReturn = tableReturn + "'" + row.columns.get(colName) + "'";
//                } else {
//                    tableReturn += row.columns.get(colName);
//                }
//                if (columnNameIterator.hasNext()) {
//                    tableReturn += ",";
//                }
//            }
//            tableReturn += "\n";
//        }
//        return tableReturn;
//    }
//}