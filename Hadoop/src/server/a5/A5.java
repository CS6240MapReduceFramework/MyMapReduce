package a5;

import hadoop.*;

import java.io.IOException;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.Tool; --
//import org.apache.hadoop.util.ToolRunner; --

/**
 * MapReduce program to calculate Missed Connections
 */
public class A5 {

    public static class MissedFlightMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * Parse a CSV record given as string
         * - Remove any quotes from record
         * - Splits on each new column
         *
         * @param record The record to parse
         * @return Array containing those records, null if couldn't parse
         */
        private String[] parse(String record, int column) {

            String[] ans = new String[column];
            int col = 0;
            int len = record.length();
            StringBuilder builder = new StringBuilder();
            char[] arr = record.toCharArray();

            for (int i = 0; i < len; i++) {
                char ch = arr[i];

                // Remove quotes
                if (ch == '\"') {
                    continue;
                }

                if (ch != ',') {
                    // No Split condition
                    builder.append(ch);

                    if (i == len - 1) {
                        ans[col] = builder.toString();
                    }
                } else if (i + 1 < len && arr[i + 1] == ' ') {
                    // Ignore condition
                    builder.append(ch);
                } else {
                    // Split condition
                    if (i == len - 1) {
                        col = 0;
                        break;
                    }

                    ans[col] = builder.toString();
                    builder.setLength(0);
                    col++;
                }
            }

            if (col < column - 1) {
                return null;
            }

            return ans;
        }

        /**
         * Checks if the flight is within or on that year
         *
         * @param row   The record for a flight
         * @param years Range of year or a particular year
         * @return true if the flight is on given date, false otherwise
         */
        private boolean checkYear(String row[], int... years) {
            int year = (int) Float.parseFloat(row[0]);

            if (years.length == 1 && year == years[0]) {
                return true;
            }

            if (years.length == 2 && year >= years[0] && year <= years[1]) {
                return true;
            }

            return false;
        }

        /**
         * Convert time in hhmm format to minutes since day started
         *
         * @param time The time in hhmm format
         * @return Minutes minutes since day started
         * @throws NumberFormatException
         */
        private int timeToMinute(String time) throws NumberFormatException {
            if (time == null || time.length() == 0) {
                throw new NumberFormatException();
            }

            int hhmm = (int) Float.parseFloat(time);
            int hour = 0;
            int minute = 0;

            if (hhmm < 100) {
                minute = hhmm;
            } else {
                hour = hhmm / 100;
                minute = hhmm % 100;
            }

            return hour * 60 + minute;
        }

        /**
         * Check whether the given record passes the sanitary test
         *
         * @param row Record of flight OTP data
         * @return true iff it passes sanity test. False, otherwise
         */
        private boolean sanityTest(String[] row) {
            try {

                // hh:mm format
                int CRSArrTime = timeToMinute(row[40]);
                int CRSDepTime = timeToMinute(row[29]);

                // Check for zero value
                if (CRSArrTime == 0 || CRSDepTime == 0) {
                    return false;
                }

                // minutes format
                int CRSElapsedTime = (int) Float.parseFloat(row[50]);
                int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;

                // Check for modulo zero
                if (timeZone % 60 != 0) {
                    return false;
                }

                int originAirportId = (int) Float.parseFloat(row[11]);
                int destAirportId = (int) Float.parseFloat(row[20]);
                int originAirportSeqId = (int) Float.parseFloat(row[12]);
                int destAirportSeqId = (int) Float.parseFloat(row[21]);
                int originCityMarketId = (int) Float.parseFloat(row[13]);
                int destCityMarketId = (int) Float.parseFloat(row[22]);
                int originStateFips = (int) Float.parseFloat(row[17]);
                int destStateFips = (int) Float.parseFloat(row[26]);
                int originWac = (int) Float.parseFloat(row[19]);
                int destWac = (int) Float.parseFloat(row[28]);

                // Check for Ids greater than zero
                if (originAirportId <= 0 || destAirportId <= 0 || originAirportSeqId <= 0 ||
                        destAirportSeqId <= 0 || originCityMarketId <= 0 || destCityMarketId <= 0 ||
                        originStateFips <= 0 || destStateFips <= 0 || originWac <= 0 || destWac <= 0) {
                    return false;
                }

                // Check for non-empty condition
                if (row[14].isEmpty() || row[23].isEmpty() || row[15].isEmpty() || row[24].isEmpty() ||
                        row[16].isEmpty() || row[25].isEmpty() || row[18].isEmpty() || row[27].isEmpty()) {
                    return false;
                }

                // For flights that are not cancelled
                int cancelled = (int) Float.parseFloat(row[47]);
                if (cancelled != 1) {
                    int arrTime = timeToMinute(row[41]);
                    int depTime = timeToMinute(row[30]);
                    int actualElapsedTime = (int) Float.parseFloat(row[51]);

                    // Check for zero value
                    int time = arrTime - depTime - actualElapsedTime - timeZone;
                    if (time != 0 && time % 1440 != 0) {
                        return false;
                    }

                    int arrDelay = (int) Float.parseFloat(row[42]);
                    int arrDelayMinutes = (int) Float.parseFloat(row[43]);
                    if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
                        return false;
                    } else if (arrDelay < 0 && arrDelayMinutes != 0) {
                        return false;
                    }

                    if (arrDelayMinutes >= 15 && ((int) Float.parseFloat(row[44])) != 1) {
                        return false;
                    }
                }

            } catch (NumberFormatException exception) {

                return false;

            }

            return true;
        }

        /**
         * Check whether the given flight is cancelled or not
         *
         * @param row Record of flight OTP data
         * @return true iff it cancelled. False, otherwise
         */
        private boolean isCancelled(String[] row) {
            int cancelled = (int) Float.parseFloat(row[47]);
            return cancelled == 1;
        }

        /**
         * Convert the given date in YYYY-MM-DD format into
         * the minutes since epoch
         *
         * @param aDate Date
         * @return Minutes since epoch
         */
        private long getEpochMinutes(String aDate) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date = null;
            try {
                date = simpleDateFormat.parse(aDate);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return date.getTime() / (long) (60 * 1000);
        }

        /**
         * Return the timezone in minutes
         *
         * @param row Record of flight OTP data
         * @return Timezone
         */
        private int getTimeZone(String[] row) {
            // hh:mm format
            int CRSArrTime = timeToMinute(row[40]);
            int CRSDepTime = timeToMinute(row[29]);

            // minutes format
            int CRSElapsedTime = (int) Float.parseFloat(row[50]);
            return CRSArrTime - CRSDepTime - CRSElapsedTime;
        }

        /**
         * Return the arrival date and time in epoch minutes
         *
         * @param depFlight   Departure flight date in epoch minutes
         * @param elapsedTime Duration of flight in minutes
         * @param depTime     Departure flight time in epoch minutes
         * @param timezone    Timezone in minutes
         * @return Arrival Time
         */
        private long getArrivalEpochMinutes(long depFlight, int elapsedTime, int depTime, int timeZone) {
            return depFlight + elapsedTime + depTime + timeZone;
        }

        /**
         * Return the departure date and time in epoch minutes
         *
         * @param depFlight Departure flight date in epoch minutes
         * @param depTime   Departure flight time in epoch minutes
         * @return Departure Time
         */
        private long getDepartureEpochMinutes(long depFlight, int depTime) {
            return depFlight + depTime;
        }

        /**
         * Return the departure delay in minutes
         *
         * @param CRSDepTime CRS departure flight time in epoch minutes
         * @param depTime    Departure flight time in epoch minutes
         * @return Departure Time
         */
        private long getDepartureDelay(int CRSDepTime, int depTime) {
            return depTime - CRSDepTime;
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] row = parse(value.toString(), 110);
            if (row != null && sanityTest(row) && !isCancelled(row)) {
                String carrierCode = row[8];
                String year = row[0];
                int timeZone = getTimeZone(row);
                int depDelay = (int) Float.parseFloat(row[31]);

                // CRS Time
                int CRSArrTime = timeToMinute(row[40]);
                int CRSDepTime = timeToMinute(row[29]);

                // Actual Time
                int arrTime = timeToMinute(row[41]);
                int depTime = timeToMinute(row[30]);

                // Get Elapsed Time
                int CRSElapsedTime = (int) Float.parseFloat(row[50]);
                int actualElapsedTime = (int) Float.parseFloat(row[51]);

                // Get Flight date
                long CRSDepFlightDate = getEpochMinutes(row[5]);
                long depFlightDate = CRSDepFlightDate + depDelay;

                // Airport Id
                String destAirportId = row[20];
                String originAirportId = row[11];

                // Get Time in epoch
                long CRSDepEpochTime = getDepartureEpochMinutes(CRSDepFlightDate, CRSDepTime);
                long depEpochTime = getDepartureEpochMinutes(depFlightDate, depTime);
                long CRSArrEpochTime = getArrivalEpochMinutes(CRSDepFlightDate, CRSElapsedTime, CRSDepTime, timeZone);
                long arrEpochTime = getArrivalEpochMinutes(depFlightDate, actualElapsedTime, depTime, timeZone);

                Text a1 = new Text();
                a1.set(carrierCode + " " + year + " " + destAirportId);
                Text a2 = new Text();
                a2.set("A:" + CRSArrEpochTime + "\t" + arrEpochTime);
                // Emit Arrival
                context.write(a1, a2);

                a1.set(carrierCode + " " + year + " " + originAirportId);
                a2.set("D:" + CRSDepEpochTime + "\t" + depEpochTime);
                // Emit Departure
                context.write(a1, a2);
            }
        }

    }

    public static class MissedFlightReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Check whether the flights are a missed connection or not
         *
         * @param CRSArrTime The scheduled arrival time
         * @param CRSDepTime The scheduled departure time
         * @return true iff they are missed connections. False, otherwise
         */
        private boolean isMissedConnection(long arrEpochTime, long depEpochTime) {
            if (depEpochTime < arrEpochTime + 30) {
                return true;
            }
            return false;
        }

        /**
         * Check whether the flights are a connection or not
         *
         * @param CRSArrTime The scheduled arrival time
         * @param CRSDepTime The scheduled departure time
         * @return true iff they are connections. False, otherwise
         */
        private boolean isConnection(long CRSArrEpochTime, long CRSDepEpochTime) {
            if (CRSDepEpochTime >= CRSArrEpochTime + 30 && CRSDepEpochTime <= CRSArrEpochTime + 360) {
                return true;
            }
            return false;
        }

        public void reduce(Text key, CustomIterable<Text> values, Context context)
                throws IOException, InterruptedException, Exception {

            /**
             * Pair for Actual and Scheduled Time
             */
            class Pair {
                long CRSTime;
                long time;
            }

            CustomIterable<Text> start = values;
            int connection = 0;
            int missedConnection = 0;
            List<Pair> arrList = new ArrayList<Pair>();
            List<Pair> depList = new ArrayList<Pair>();

            // Fill List
            while(values.hasNext()){
                Text value = values.next();
                String[] data = value.toString().split(":");
                String[] flightInfo = data[1].split("\t");
                String CRSTime = flightInfo[0];
                String time = flightInfo[1];
                Pair pair = new Pair();
                pair.CRSTime = Long.parseLong(CRSTime);
                pair.time = Long.parseLong(time);

                if (data[0].equals("A")) {
                    arrList.add(pair);
                } else {
                    depList.add(pair);
                }
            }

            // Find Connections
            for (Pair arrPair : arrList) {
                long CRSArrTime = arrPair.CRSTime;
                long arrTime = arrPair.time;

                for (Pair depPair : depList) {
                    long CRSDepTime = depPair.CRSTime;
                    long depTime = depPair.time;
                    if (isConnection(CRSArrTime, CRSDepTime)) {
                        connection++;
                        if (isMissedConnection(arrTime, depTime)) {
                            missedConnection++;
                        }
                    }
                }
            }
            Text temp = new Text();
            temp.set(missedConnection + "\t" + connection);
            context.write(key, temp);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = null;
        try {
            Configuration conf = new Configuration();
            job = Job.getInstance(conf, "csv");
//		    job.setJarByClass(Main.class);
            job.setMapperClass(MissedFlightMapper.class);
            job.setReducerClass(MissedFlightReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // FileInputFormat.addInputPath(job, args[0]);
            // FileOutputFormat.setOutputPath(job,args[1]);

            job.waitForCompletion(true);
            System.out.println(job.getJobname() + " job completed successfully!");
        } catch (Exception e) {
            System.out.println(job.getJobname() + " job failed!!");
            e.printStackTrace();
        }
    }

}