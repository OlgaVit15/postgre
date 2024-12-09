package org.example;

import org.postgresql.util.ByteConverter;

import java.awt.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;

public class Read {
    int byteArrToInt (byte[] b){
        int n = 0;
        for (int i = 0; i < b.length; i++) {
            if (i+1 == b.length) {
                n += ((b[i] & 0xFF) << 8);
            }
            n += (b[i] & 0xFF);
        }
        return n;
    }
    Map<String, List<String>> getschema (String name) throws SQLException {
        java.sql.Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "postgres");
        String query = "select attnum, typname, typlen  from pg_class c \n" +
                "inner join pg_attribute a on c.oid = a.attrelid\n" +
                "inner join pg_type t on a.atttypid = t.oid\n" +
                "WHERE attnum > 0 and c.relfilenode = " + name + " order by attnum";
        PreparedStatement stm = conn.prepareStatement(query);
        ResultSet res = stm.executeQuery();
        Map<String, List<String>> schema = new HashMap<>();
        while(res.next()) {
            List<String> types = new ArrayList<>();
            types.add(res.getString(2));
            types.add(res.getString(3));
            schema.put(res.getString(1), types);
        }
        conn.close();
        return schema;
    }
    byte[] byteListToArr(List<Byte> b){
        byte[] tr = new byte[b.size()];
        for (int i = 0; i < b.size(); i++){
            tr[i] = b.get(i);
        }
        return tr;
    }
    int bitToint(BitSet bs, int length){
        int i = 0;
        for (int bit = 0; bit < length; bit++) {
            if (bs.get(bit)) {
                i |= (1 << bit);
            }
        }
        return i;
    }

    List<Tupple> getTuplePositions (byte[] itemSeq){
        List<byte[]> itemIds = new ArrayList<>();
        List<Byte> buf = new ArrayList<>();
        for (byte b : itemSeq){
            buf.add(b);
            if (buf.size() == 4){
                itemIds.add(byteListToArr(buf));
                buf.clear();
            }
        }

        List<Tupple> positions = new ArrayList<>();
        for (int i = 0; i < itemIds.size(); i++){
            Tupple tupple = new Tupple();
            BitSet bs = BitSet.valueOf(itemIds.get(i));
            BitSet tl = bs.get(17, bs.length());
            tupple.offset = bitToint(bs, 15);
            tupple.length = bitToint(tl, tl.length());
            positions.add(tupple);
        }
        return positions;
    }

    List<byte[]> getRows (List<Tupple> tupples, byte[] data){
        List<byte[]> rows = new ArrayList<>();
        for(Tupple t : tupples) {
            byte[] tupple = new byte[t.length];
            for (int i = t.offset; i < t.offset + t.length; ) {
                for (int j = 0; j < t.length; j++) {
                    tupple[j] = data[i];
                    i++;
                }
            }
            rows.add(tupple);
        }
        return rows;
    }

    List getValues(String link, byte[] ty) throws SQLException {
        List buffer = new ArrayList<>();

        byte[] upd = Arrays.copyOfRange(ty, 12, 18);
        int upd_off = byteArrToInt(upd);
        byte[] info = new byte[]{ty[20]};
        BitSet fl = BitSet.valueOf(info);
        boolean nullfl = fl.get(0);

        byte[] info2 = new byte[]{ty[18], ty[19]};
        int attrNum = bitToint(BitSet.valueOf(info2), 11);

        int start = (ty[22] & 0xFF);
        byte[] bitmap= Arrays.copyOfRange(ty, 23, start);;
        BitSet f = BitSet.valueOf(bitmap);
        BitSet bmap = f.get(0, attrNum);

        byte[] rowdata = Arrays.copyOfRange(ty, start, ty.length);
        Map<String, List<String>> schema = getschema(link);

        byte[] coll;
        int pointer = 0;
        String str;
        int in;
        boolean bool;
        boolean ifnull;
        for (int i = 0; i < attrNum; i++){
            if (nullfl){
                ifnull = bmap.get(i);
            }
            else{
                ifnull = true;
            }
            if(ifnull){
                String num = String.valueOf(i+1);
                switch(schema.get(num).get(0)){
                    case "varchar":

                        byte[] varln = Arrays.copyOfRange(rowdata, pointer, pointer+4);
                        BitSet var = BitSet.valueOf(varln);
                        int datalen = 0;
                        //определяем заголовок строки
                        if (var.get(0)) {
                            datalen = ((rowdata[pointer] & 0xFF) >>> 1);
                            coll = Arrays.copyOfRange(rowdata, pointer+1, pointer + datalen);
                        }
                        else{
                            datalen = bitToint(var.get(2, var.length()), var.length());
                            coll = Arrays.copyOfRange(rowdata, pointer+4, pointer + datalen);
                        }
                        str = new String(coll, StandardCharsets.UTF_8);
                        buffer.add(str);
                        pointer += datalen;
                        break;

                    case "int4":
                        if (pointer % 4 != 0) {
                            while(pointer % 4 != 0){
                                pointer++;
                            }
                        }
                        coll = Arrays.copyOfRange(rowdata, pointer, pointer + 4);
                        in = ByteBuffer.wrap(coll).get();
                        pointer += 4;
                        buffer.add(in);
                        break;

                    case "date":

                        coll =  Arrays.copyOfRange(rowdata, pointer, pointer + 4);
                        int d = ByteBuffer.wrap(coll).get();
                        LocalDate baseDate = LocalDate.of(2000, 1, 1);
                        LocalDate resultDate = baseDate.plusDays(d);
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");
                        buffer.add(resultDate.format(formatter));
                        break;
                }
            }
            else{
                buffer.add("null");
            }
        }
        buffer.add(upd_off);
        return buffer;
    }

    void read(String path, String fnode) throws IOException, SQLException {
        byte[] data = Files.readAllBytes(Path.of(path));

        int lower = (data[12] & 0xFF)
                + ((data[13] & 0xFF)<< 8);
        int upper = (data[14] & 0xFF)
                + ((data[15] & 0xFF)<< 8);
        int special = (data[16] & 0xFF)
                + ((data[17] & 0xFF)<< 8);

        byte[] itemSeq = Arrays.copyOfRange(data, 24, lower);
        List<Tupple> positions = getTuplePositions(itemSeq);

        List<byte[]> rows = getRows(positions, data);

        List<List> table = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++){
            List row;
            row = getValues(fnode, rows.get(i));
            table.add(row);
        }

        for (List row : table){
            for (int i = 0; i< row.size()-1; i++) {
                System.out.print(" column" + i + ": " + row.get(i));
            }
            System.out.println("\n");
        }

        System.out.println(table.toString());
    }
}
