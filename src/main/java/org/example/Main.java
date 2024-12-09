package org.example;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        Read r = new Read();//16441
        r.read("C://Program Files//PostgreSQL//16//data//base//5//16734", "16734");
    }
}