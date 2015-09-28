package com.fuzzy.load;

import com.fuzzy.model.ConnectDB;
import com.mysql.jdbc.Connection;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

/**
 *
 * @author TRAN HUU DAT
 */
public class LoadFunction {

    public BufferedReader loadStream(String fileName) throws FileNotFoundException {
        BufferedReader bf = new BufferedReader(new FileReader(fileName));
        return bf;
    }

    //sinh luat
    public void loadToDB(String fileName) throws FileNotFoundException, IOException, SQLException {
        int age, fnlwgt, edu_num, cap_gain, cap_loss, hour;
        String workclass, edu, mari, occup, realtion, race, sex, country, predict;
        float depend;
        ConnectDB db = new ConnectDB();
        try (Connection conn = db.getConnecttion()) {
            if (conn == null) {
                System.out.println("can't not connect db");
                return;
            }
            conn.setAutoCommit(false);
            //query insert data rules
            //String queryInsert = "INSERT INTO `data_input`( `age`, `workclass`, `fnlwgt`, `education`, `education_num`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict`, `depen_value`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            //query insert data test
            String queryInsert = "INSERT INTO `test_input`( `age`, `workclass`, `fnlwgt`, `education`, `education_num`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement insertState = conn.prepareStatement(queryInsert);
            BufferedReader input = loadStream(fileName);
            String line;
            int i = 0;
            while ((line = input.readLine()) != null) {
                if (line.contains("?")) {
                    continue;
                }
                StringTokenizer token = new StringTokenizer(line, ",");
                if (token.countTokens() != 15) {
                    continue;
                }
                age = Integer.parseInt(token.nextToken().trim());
                workclass = token.nextToken().trim();
                fnlwgt = Integer.parseInt(token.nextToken().trim());
                edu = token.nextToken().trim();
                edu_num = Integer.parseInt(token.nextToken().trim());
                mari = token.nextToken().trim();
                occup = token.nextToken().trim();
                realtion = token.nextToken().trim();
                race = token.nextToken().trim();
                sex = token.nextToken().trim();
                cap_gain = Integer.parseInt(token.nextToken().trim());
                cap_loss = Integer.parseInt(token.nextToken().trim());
                hour = Integer.parseInt(token.nextToken().trim());
                country = token.nextToken().trim();
                predict = token.nextToken().trim();
                insertState.setInt(1, age);
                insertState.setString(2, workclass);
                insertState.setInt(3, fnlwgt);
                insertState.setString(4, edu);
                insertState.setInt(5, edu_num);
                insertState.setString(6, mari);
                insertState.setString(7, occup);
                insertState.setString(8, realtion);
                insertState.setString(9, race);
                insertState.setString(10, sex);
                insertState.setInt(11, cap_gain);
                insertState.setInt(12, cap_loss);
                insertState.setInt(13, hour);
                insertState.setString(14, country);
                insertState.setString(15, predict);
                insertState.executeUpdate();
                conn.commit();
                if (i % 50 == 0) {
                    System.out.println(i);
                }
                i++;
            }
            conn.setAutoCommit(true);
            if (insertState != null) {
                insertState.close();
            }
        }
    }

    public void xuLiMauThuan() throws IOException, SQLException {
        int age, cap_gain, cap_loss, hour, id;
        String workclass, edu, mari, occup, realtion, race, sex, country;

        HashMap<String, ArrayList> map = new HashMap<>();
        Calulate cal = new Calulate();

        float depend;
        ConnectDB db = new ConnectDB();
        Connection conn = db.getConnecttion();
        if (conn == null) {
            System.out.println("can't not connect db");
            return;
        }
        String queryString = "select `id`,`age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `depen_value` from rules_data";

        Statement query = conn.createStatement();
        ResultSet rs = query.executeQuery(queryString);
        //
        while (rs.next()) {
            id = rs.getInt("id");
            age = rs.getInt("age");
            cap_gain = rs.getInt("cap_gain");
            cap_loss = rs.getInt("cap_loss");
            hour = rs.getInt("hour_per_week");
            workclass = rs.getString("workclass");
            edu = rs.getString("education");
            mari = rs.getString("marital_status");
            occup = rs.getString("occupation");
            realtion = rs.getString("relationship");
            race = rs.getString("race");
            sex = rs.getString("sex");
            country = rs.getString("country");
            float depen_value = rs.getFloat("depen_value");
            String key = age + workclass + edu + mari + occup + realtion + race + sex + cap_gain + cap_loss + hour + country;

            if (map.containsKey(key)) {
                ArrayList temp = map.get(key);// array<Int, float> <=> (id, depen);
                float depen = (float) temp.get(1);//tra lai depen cua phan tu co khoa key trong map
                if (depen_value > depen) {
                    temp.set(0, id);
                    temp.set(1, depen_value);
                    map.put(key, temp);
                }
            } else {
                ArrayList temp = new ArrayList();
                temp.add(id);
                temp.add(depen_value);
                map.put(key, temp);
            }

        }
        System.out.println("read data complete.....");

        //begin update
        String updateString = "update rules_data set active=? where id=?";
        PreparedStatement updateState = conn.prepareStatement(updateString);
        Iterator entries = map.entrySet().iterator();
        while (entries.hasNext()) {
            Entry thisEntry = (Entry) entries.next();
            ArrayList value = (ArrayList) thisEntry.getValue();
            updateState.setInt(1, 1);
            updateState.setInt(2, (int) value.get(0));
            updateState.executeUpdate();

        }
        updateState.close();
        conn.close();

    }

    public void generateRules() throws SQLException, IOException {
        int age, cap_gain, cap_loss, hour;
        String workclass, edu, mari, occup, realtion, race, sex, country, predict;

        ArrayList<ArrayList> result = new ArrayList<>();
        Calulate cal = new Calulate();

        float depend;
        ConnectDB db = new ConnectDB();
        Connection conn = db.getConnecttion();
        if (conn == null) {
            System.out.println("can't not connect db");
            return;
        }
        String queryString = "select `age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict` from data_input";
        String insertRules = "INSERT INTO `rules_data`( `age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict`, `depen_value`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //----------------------------------------begin select

        Statement query = conn.createStatement();
        ResultSet rs = query.executeQuery(queryString);

        while (rs.next()) {
            age = rs.getInt("age");
            cap_gain = rs.getInt("cap_gain");
            cap_loss = rs.getInt("cap_loss");
            hour = rs.getInt("hour_per_week");
            workclass = rs.getString("workclass");
            edu = rs.getString("education");
            mari = rs.getString("marital_status");
            occup = rs.getString("occupation");
            realtion = rs.getString("relationship");
            race = rs.getString("race");
            sex = rs.getString("sex");
            country = rs.getString("country");
            predict = rs.getString("predict");
            ArrayList temp = new ArrayList();
            temp.add(cal.getDegree(age, Calulate.AGE));
            temp.add(workclass);
            temp.add(edu);
            temp.add(mari);
            temp.add(occup);
            temp.add(realtion);
            temp.add(race);
            temp.add(sex);
            temp.add(cal.getDegree(cap_gain, Calulate.CAPITAL_GAIN));
            temp.add(cal.getDegree(cap_loss, Calulate.CAPITAL_LOSS));
            temp.add(cal.getDegree(hour, Calulate.HOUR_PER_WEEK));
            temp.add(country);
            temp.add(predict);
            float depen = cal.getDepenOfRule(age, hour, cap_gain, cap_loss);
            temp.add(depen);
            result.add(temp);
        }
        System.out.println("select complete ......");

        PreparedStatement insertState = conn.prepareStatement(insertRules);
        for (int i = 0; i < result.size(); i++) {
            ArrayList temp = result.get(i);
            insertState.setInt(1, (int) temp.get(0));
            insertState.setString(2, (String) temp.get(1));
            insertState.setString(3, (String) temp.get(2));
            insertState.setString(4, (String) temp.get(3));
            insertState.setString(5, (String) temp.get(4));
            insertState.setString(6, (String) temp.get(5));
            insertState.setString(7, (String) temp.get(6));
            insertState.setString(8, (String) temp.get(7));
            insertState.setInt(9, (int) temp.get(8));
            insertState.setInt(10, (int) temp.get(9));
            insertState.setInt(11, (int) temp.get(10));
            insertState.setString(12, (String) temp.get(11));
            insertState.setString(13, (String) temp.get(12));
            insertState.setFloat(14, (float) temp.get(13));
            insertState.executeUpdate();
        }
        query.close();
        insertState.close();
        conn.close();

    }

    public void addDepenOfRule() throws SQLException, IOException {
        int age, fnlwgt, edu_num, cap_gain, cap_loss, hour;
        float depend = 0;

        //create array to save depen
        ArrayList<Float> ResultDepens = new ArrayList<>();
        // create calulate object to cal depend;
        Calulate cal = new Calulate();
        //create connect to db
        ConnectDB db = new ConnectDB();
        Connection conn = db.getConnecttion();
        if (conn == null) {
            System.out.println("can't not connect db");
            return;
        }
        //create sql preparestament
        String queryData = "select age, fnlwgt,cap_gain, cap_loss, hour_per_week,education_num from data_input";
        String insertDepen = "update data_input set depen_value=? where id=?";

        //begin transaction
        //select data
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(queryData);
        int numColumns = rs.getMetaData().getColumnCount();
        System.out.println("read data begin .....");
        int count = 0;
        while (rs.next()) {
            age = rs.getInt("age");
            fnlwgt = rs.getInt("fnlwgt");
            cap_gain = rs.getInt("cap_gain");
            cap_loss = rs.getInt("cap_loss");
            hour = rs.getInt("hour_per_week");
            edu_num = rs.getInt("education_num");
//          depend = cal.getDepenOfRule(age, fnlwgt, cap_gain, cap_loss, hour, edu_num);
            float depen = 0;
            ResultDepens.add(depend);
            if (count % 100 == 0) {
                System.out.println(count);
            }
            count++;
        }
        System.out.println("read complete....");
        //------------------------------------
        System.out.println("insert data begin.....");
        conn.setAutoCommit(false);
        PreparedStatement ins = conn.prepareStatement(insertDepen);
        for (int i = 0; i < ResultDepens.size(); i++) {
            ins.setFloat(1, ResultDepens.get(i));
            ins.setInt(2, i + 15257);
            ins.executeUpdate();
            conn.commit();
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }
        conn.setAutoCommit(true);
        ins.close();
        conn.close();
    }

    //sinh bo test
    public void sinhTestChuan() throws SQLException, IOException {
        int age, fnlwgt, edu_num, cap_gain, cap_loss, hour;
        String workclass, edu, mari, occup, realtion, race, sex, country, predict;

        ArrayList<ArrayList> result = new ArrayList<>();
        Calulate cal = new Calulate();

        ConnectDB db = new ConnectDB();
        Connection conn = db.getConnecttion();
        if (conn == null) {
            System.out.println("can't not connect db");
            return;
        }
        String queryString = "select `age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict` from test_input";
        String insertRules = "INSERT INTO `test_data`( `age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";
        //----------------------------------------begin select

        Statement query = conn.createStatement();
        ResultSet rs = query.executeQuery(queryString);

        while (rs.next()) {
            age = rs.getInt("age");
            cap_gain = rs.getInt("cap_gain");
            cap_loss = rs.getInt("cap_loss");
            hour = rs.getInt("hour_per_week");
            workclass = rs.getString("workclass");
            edu = rs.getString("education");
            mari = rs.getString("marital_status");
            occup = rs.getString("occupation");
            realtion = rs.getString("relationship");
            race = rs.getString("race");
            sex = rs.getString("sex");
            country = rs.getString("country");
            predict = rs.getString("predict");
            ArrayList temp = new ArrayList();
            temp.add(cal.getDegree(age, Calulate.AGE));
            temp.add(workclass);
            temp.add(edu);
            temp.add(mari);
            temp.add(occup);
            temp.add(realtion);
            temp.add(race);
            temp.add(sex);
            temp.add(cal.getDegree(cap_gain, Calulate.CAPITAL_GAIN));
            temp.add(cal.getDegree(cap_loss, Calulate.CAPITAL_LOSS));
            temp.add(cal.getDegree(hour, Calulate.HOUR_PER_WEEK));
            temp.add(country);
            temp.add(predict);
            result.add(temp);
        }
        System.out.println("select complete ......");

        PreparedStatement insertState = conn.prepareStatement(insertRules);
        for (int i = 0; i < result.size(); i++) {
            ArrayList temp = result.get(i);
            insertState.setInt(1, (int) temp.get(0));
            insertState.setString(2, (String) temp.get(1));
            insertState.setString(3, (String) temp.get(2));
            insertState.setString(4, (String) temp.get(3));
            insertState.setString(5, (String) temp.get(4));
            insertState.setString(6, (String) temp.get(5));
            insertState.setString(7, (String) temp.get(6));
            insertState.setString(8, (String) temp.get(7));
            insertState.setInt(9, (int) temp.get(8));
            insertState.setInt(10, (int) temp.get(9));
            insertState.setInt(11, (int) temp.get(10));
            insertState.setString(12, (String) temp.get(11));
            insertState.setString(13, (String) temp.get(12));
            insertState.executeUpdate();
        }
        insertState.close();
        conn.close();

    }

    public void test() throws IOException, SQLException {
        int age, cap_gain, cap_loss, hour, id;
        String workclass, edu, mari, occup, realtion, race, sex, country, predict;

        HashMap<String, String> map = new HashMap<>();//map of rules
        Calulate cal = new Calulate();

        ConnectDB db = new ConnectDB();
        Connection conn = db.getConnecttion();
        if (conn == null) {
            System.out.println("can't not connect db");
            return;
        }
        String queryString = "select `age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict` from `rules_data` where `active`=1";

        Statement query = conn.createStatement();
        ResultSet rs = query.executeQuery(queryString);
        rs.afterLast();
        System.out.println(rs.getRow());
        rs.beforeFirst();
        // get tap luat
        while (rs.next()) {
            age = rs.getInt("age");
            cap_gain = rs.getInt("cap_gain");
            cap_loss = rs.getInt("cap_loss");
            hour = rs.getInt("hour_per_week");
            workclass = rs.getString("workclass");
            edu = rs.getString("education");
            mari = rs.getString("marital_status");
            occup = rs.getString("occupation");
            realtion = rs.getString("relationship");
            race = rs.getString("race");
            sex = rs.getString("sex");
            country = rs.getString("country");
            predict = rs.getString("predict");
            String key = age + workclass + edu + mari + occup + realtion + race + sex + cap_gain + cap_loss + hour + country;
            map.put(key, predict);
            
        }
        System.out.println("read data rules complete....." + rs.getRow());

        //read data test
        ArrayList<Integer> resultTrue = new ArrayList<>();
        String queryStringTest = "select `id`,`age`, `workclass`, `education`, `marital_status`, `occupation`, `relationship`, `race`, `sex`, `cap_gain`, `cap_loss`, `hour_per_week`, `country`, `predict` from test_data";
        Statement queryTest = conn.createStatement();
        ResultSet rs1 = queryTest.executeQuery(queryStringTest);
        // get tap test
        int count = 0;
        while (rs1.next()) {
            id = rs1.getInt("id");
            age = rs1.getInt("age");
            cap_gain = rs1.getInt("cap_gain");
            cap_loss = rs1.getInt("cap_loss");
            hour = rs1.getInt("hour_per_week");
            workclass = rs1.getString("workclass");
            edu = rs1.getString("education");
            mari = rs1.getString("marital_status");
            occup = rs1.getString("occupation");
            realtion = rs1.getString("relationship");
            race = rs1.getString("race");
            sex = rs1.getString("sex");
            country = rs1.getString("country");
            predict = rs1.getString("predict");
            String key = age + workclass + edu + mari + occup + realtion + race + sex + cap_gain + cap_loss + hour + country;
//            if (map.containsKey(key) && map.get(key).equals(predict)) {
//                resultTrue.add(id);
//            }
            if(map.containsKey(key))
                count++;
        }
        System.out.println("read data test complete....."+ count);

        //begin update
        String updateString = "update test_data set active=? where id=?";
        PreparedStatement updateState = conn.prepareStatement(updateString);
        Iterator entries = resultTrue.iterator();
        while (entries.hasNext()) {
            int value = (int) entries.next();
            updateState.setInt(1, 1);
            updateState.setInt(2, value);
            updateState.executeUpdate();

        }
        query.close();
        queryTest.close();
        updateState.close();
        conn.close();
        System.out.println("close connect");
    }

    public static void main(String[] args) throws SQLException, IOException {
        LoadFunction a = new LoadFunction();
        
        String fileTest = "adult.test";
        // a.loadToDB(fileTest);
       // a.generateRules();
        //a.xuLiMauThuan();
//        a.sinhTestChuan();
        a.test();
    }

}
