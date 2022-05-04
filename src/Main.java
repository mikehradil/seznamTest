import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Main {

    /** Connects and inserts data to the MYSQL database
     *
     * @param mergedList
     */
    static void connectDB(ArrayList<ArrayList<String>> mergedList) {
        String url = "jdbc:mysql://localhost:3306/seznamDB";
        String username = "root";
        String password = "password";

        try {
            Connection connection = DriverManager.getConnection(url, username, password);

            String sql = "INSERT INTO impressions (impression_time, impression_id, ad_id, visitor_hash, click_time) VALUES(?, ?, ?, ?, ?);";
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            for (int i = 1; i < mergedList.size(); i++) {
                PreparedStatement statement = connection.prepareStatement(sql);

                Date impressionTime = formatter.parse(mergedList.get(i).get(0));
                statement.setTimestamp(1, new java.sql.Timestamp(impressionTime.getTime()));
                statement.setInt(2, Integer.parseInt(mergedList.get(i).get(1)));
                statement.setInt(3, Integer.parseInt(mergedList.get(i).get(2)));
                statement.setString(4, mergedList.get(i).get(3));

                // checks if click_time is null and if is not, addsthe data
                if (mergedList.get(i).get(4) != null) {
                    Date clickTime = formatter.parse(mergedList.get(i).get(4));

                    statement.setTimestamp(5, new java.sql.Timestamp(clickTime.getTime()));
                } else {
                    statement.setNull(5, java.sql.Types.INTEGER);
                }
                statement.executeUpdate();
                statement.close();
            }

            connection.close();
            System.out.println("Successfully connected and inserted data to the database.");
        } catch (SQLException | ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /** Checks for multiple clicks and deletes them
     *
     * @param mergedList
     */
    static void deleteMultipleClicks(ArrayList<ArrayList<String>> mergedList) {
        for (int i = 0; i < mergedList.size(); i++) {
            for (int j = 0; j < i; j++) {
                if ((mergedList.get(i).get(3).equals(mergedList.get(j).get(3))) && ((mergedList.get(i).get(2)).equals(mergedList.get(j).get(2))) && ((mergedList.get(i).get(4) != null) && (mergedList.get(j).get(4) != null))) {
                    if (compareDates(mergedList.get(i).get(4), mergedList.get(j).get(4))) {
                        mergedList.remove(i);
                    }
                }
            }
        }
    }

    /** Merging to arrays into one ArrayList of ArrayLists
     *
     * @param array1
     * @param array2
     * @param mergedArray
     * @param list
     */
    static void mergeCSV(String[][] array1, String[][] array2, String[][] mergedArray, ArrayList<ArrayList<String>> list) {
        // adding clickTimeStamps values
        for (int i = 0; i < array2.length; i++) {
            for (int j = 0; j < array2[0].length; j++) {
                mergedArray[i][j] = array2[i][j];
            }
        }
        mergedArray[0][4] = "clickTimeStamp";

        // adding both csv data to just one ArrayList
        boolean duplicate = false;
        for (int i = 0; i < array2.length; i++) {
            duplicate = false;

            for (int j = 0; j < array1.length; j++) {
                if ((array2[i][1].equals(array1[j][1])) && !(array1[j][1].equals("impressionId"))) {
                    mergedArray[i][4] = array1[j][0];

                    ArrayList<String> lineW = new ArrayList<String>();
                    lineW.addAll(Arrays.asList(mergedArray[i]));
                    list.add(lineW);
                    duplicate = true;
                }
            }
            if (!duplicate) {
                ArrayList<String> lineW = new ArrayList<String>();
                lineW.addAll(Arrays.asList(mergedArray[i]));
                list.add(lineW);
            }
        }
    }

    /** Compares 2 dates
     *
     * @param date1
     * @param date2
     * @return True, if the difference is 10 and more minutes, False if it's less
     */
    static boolean compareDates(String date1, String date2) {

        SimpleDateFormat format = new SimpleDateFormat("yy-MM-dd HH:mm:ss");

        Date d1 = null;
        Date d2 = null;

        try {
            d1 = format.parse(date1);
            d2 = format.parse(date2);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        long diff = d2.getTime() - d1.getTime();
        long minutes = TimeUnit.MILLISECONDS.toMinutes(diff);

        if ((minutes < 10) && (minutes > -10)) {
            return true;
        } else {
            return false;
        }
    }

    /** Reads CSV file and adds it to the list
     *
     * @param path
     * @param lines
     */
    static void readCSV(String path, List lines) {
        BufferedReader reader = null;
        String line;

        try {
            reader = new BufferedReader(new FileReader(path));
            while ((line = reader.readLine()) != null) {
                String[] row = line.split(",");
                lines.add(line.split(","));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws Exception {
        // reading and loading first csv file
        List<String[]> lines1 = new ArrayList<String[]>();
        readCSV("src\\clicks.csv", lines1);
        String[][] array1 = new String[lines1.size()][0];
        lines1.toArray(array1);

        // reading and loading second csv file
        List<String[]> lines2 = new ArrayList<String[]>();
        readCSV("src\\impressions.csv", lines2);
        String[][] array2 = new String[lines2.size()][0];
        lines2.toArray(array2);

        String[][] finalArray = new String[array2.length][array2[0].length + 1];
        ArrayList<ArrayList<String>> mergedList = new ArrayList<ArrayList<String>>(5);

        mergeCSV(array1, array2, finalArray, mergedList);

        deleteMultipleClicks(mergedList);

        connectDB(mergedList);
    }
}

