package com.fuzzy.load;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 *
 * @author TRAN HUU DAT
 */
public class Calulate {

    private int min, max, mid;
    private HashMap<Integer, ArrayList<Integer>> properties;
    public static final int AGE = 1;
    public static final int CAPITAL_GAIN = 2;
    public static final int CAPITAL_LOSS = 3;
    public static final int HOUR_PER_WEEK = 4;

    public Calulate() throws IOException {
        properties = new HashMap<>();
        setProperties();
    }

    private void setProperties() throws FileNotFoundException, IOException {
        LoadFunction load = new LoadFunction();
        BufferedReader bf = load.loadStream("u.txt");
        bf.readLine();
        String line;
        int i = 1;
        while ((line = bf.readLine()) != null) {
            StringTokenizer token = new StringTokenizer(line, ",");
            if (token.countTokens() < 3) {
                continue;
            }
            ArrayList<Integer> vec = new ArrayList<>();
            while (token.hasMoreTokens()) {
                vec.add(Integer.parseInt(token.nextToken().trim()));
            }
            properties.put(i, vec);
            i++;
        }
        bf.close();
    }

    /**
     * function to get dependence of property
     *
     * @param x value input
     * @param typePro : type propety need get depen
     * @return value depen of x
     */
    public float getDepenOfPro(int x, int min, int max) {
        //min = properties.get(typePro).get(0);
        int mid = (min + max) / 2;
        float value;
        if (x == mid) {
            return 1;
        } else if (x >= min && x < mid) {
            return (x - min * 1.0f) / (mid - min);
        } else if (x > mid && x <= max) {
            return (max - x * 1.0f) / (max - mid);
        }
        return 0;
    }

    public float getDepenOfPro1(int x, int value1, int value2) {
        if (value1 == value2) {
            return 1;
        }
        return (value2 - x * 1.0f) / (value2 - value1);
    }

    /**
     * tra lai do phu thuoc cua gia tri x ung voi tuoi
     *
     * @param x
     * @return
     */
    public float getDepenOfAge(int x) {
        ArrayList<Integer> list = properties.get(AGE);
        int leng = list.size();
        for (int i = 1; i < leng; i += 2) {
            if (list.get(i) >= x) {
                if (x > list.get(1) && x < list.get(leng - 2)) {
                    return getDepenOfPro(x, list.get(i - 1), list.get(i));
                } else {
                    return getDepenOfPro1(x, list.get(i - 1), list.get(i));
                }
            }
        }
        return -1;
    }

    public float getDepenOfHourOfWeek(int x) {
        ArrayList<Integer> list = properties.get(HOUR_PER_WEEK);
        int leng = list.size();
        for (int i = 1; i < leng; i += 2) {
            if (list.get(i) >= x) {
                if (x > list.get(1) && x < list.get(leng - 2)) {
                    return getDepenOfPro(x, list.get(i - 1), list.get(i));
                } else {
                    return getDepenOfPro1(x, list.get(i - 1), list.get(i));
                }
            }
        }
        return -1;
    }

    public float getDepenOfCap_again(int x) {
        ArrayList<Integer> list = properties.get(CAPITAL_GAIN);
        int leng = list.size();
        for (int i = 1; i < leng; i += 2) {
            if (list.get(i) >= x) {
                return getDepenOfPro1(x, list.get(i - 1), list.get(i));
            }
        }
        return -1;
    }
    public float getDepenOfCap_loss(int x) {
        ArrayList<Integer> list = properties.get(CAPITAL_GAIN);
        int leng = list.size();
        for (int i = 1; i < leng; i += 2) {
            if (list.get(i) >= x) {
                return getDepenOfPro1(x, list.get(i - 1), list.get(i));
            }
        }
        return -1;
    }

    public float getDepenOfRule(int age, int hour, int cap_gain, int cap_loss){
         float x1 = getDepenOfAge(age) >= getDepenOfHourOfWeek(hour) ? getDepenOfHourOfWeek(hour) : getDepenOfAge(age);
         float x2 = getDepenOfCap_again(cap_gain) >= getDepenOfCap_loss(cap_loss) ?getDepenOfCap_loss(cap_loss):  getDepenOfCap_again(cap_gain);
         return x1 >= x2 ? x2 : x1;
    }
    /**
     * anh xa tu gia tri ro x sang tapj mo
     */
    public int getDegree(int x, int TypePro) {
         ArrayList<Integer> list = properties.get(TypePro);
         for(int i = 1 ; i < list.size(); i+=2){
             if(x <= list.get(i))
                 return (i+1)/2;
         }
         return -1;
    }

  

    public static void main(String[] args) throws IOException {
        Calulate a = new Calulate();
        float x = a.getDegree(3000, CAPITAL_GAIN);
        System.out.println(x);
    }

}
