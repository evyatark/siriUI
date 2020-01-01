package org.hasadna.gtfs.service;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.DayOfWeek;
import java.time.LocalDate;

import static org.hasadna.gtfs.service.Stops.decideGtfsFileName;

@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalendarReader {

    @Value("${gtfsZipFileDirectory}")
    public String directoryOfGtfsFile;

    public String theDate = "";

    Map<String, List<String>> map = HashMap.empty();

    private static final Logger logger = LoggerFactory.getLogger(CalendarReader.class);

    public CalendarReader() {
    }

    private CalendarReader(String _directoryOfGtfsFile) {
        super();
        setDirectoryOfGtfsFile(_directoryOfGtfsFile);
    }

    public static CalendarReader make(String _directoryOfGtfsFile) {
        return new CalendarReader(_directoryOfGtfsFile);
    }

    public static int dayOfWeek(String aDate) {
        if (StringUtils.isEmpty(aDate)) {
            return -1;
        }
        DayOfWeek dayOfWeek = LocalDate.parse(aDate).getDayOfWeek();
        int day = 1 + dayOfWeek.getValue();    // dayOfWeek.getValue() return day-of-week, from 1 (Monday) to 7 (Sunday)
        if (day == 8) {
            day = 1;
        }
        return day; // Sunday=1, ..., Saturday=7
    }

    public String f(String line) {
        return line.split(",")[0];
    }

    public Map<String, List<String>> readAllCalendar(String date) {
        final String gtfsZipFileName = decideGtfsFileName(date);
        final String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
        final ReadZipFile rzf = new ReadZipFile();
        Map<String, List<String>> mapServiceIdToCalendarLines = rzf
                .calendarLinesFromFile(gtfsZipFileFullPath).toList()
                .groupBy(line -> line.split(",")[0]);
        /** mapServiceIdToCalendarLines:
                ("19900" -> "19900,0,0,0,0,0,1,0,20191015,20191019"),
                ("19901" -> "19901,0,0,0,0,0,0,1,20191015,20191019" ),
                ("19902" -> "19902,0,0,1,1,1,0,0,20191015,20191019" ),
                ("19903" -> "19903,0,0,0,0,0,1,0,20191015,20191019" ),
                ("19904" -> "19904,0,0,0,0,0,0,1,20191015,20191019" ),
                ("19905" -> "19905,1,0,0,0,0,0,0,20191020,20191020" ),
                ("19906" -> "19906,0,0,1,0,0,0,0,20191022,20191022" ),
                ("19907" -> "19907,0,1,0,0,0,0,0,20191021,20191021" ),
                ("19908" -> "19908,0,1,0,0,0,0,0,20191021,20191021" ),
                ("19909" -> "19909,1,0,0,0,0,0,0,20191020,20191020")
         **/
        return mapServiceIdToCalendarLines;
    }

    public Map<String, List<String>> readCalendar(String date) {
        if (map.isEmpty()) {
            synchronized (map) {
                if (map.isEmpty()) {
                    map = readAllCalendar(date);
                    theDate = date;
                }
            }
        }
        return map;
    }

    public static List<String> linesContainDate(List<String> lines, String date) {
        return lines.filter(line -> lineContainsDate(line, date));
    }

    public static List<String> linesContainDateAndDayOfWeek(final List<String> lines, final String date) {
        final int dayOfWeek = dayOfWeek(date);
        final List<String> linesWithDay = lines
                .filter(line -> lineContainsDate(line, date))
                .filter(line -> lineContainsDayOfWeek(line, dayOfWeek));
        return linesWithDay;
    }

    private static boolean lineContainsDayOfWeek(String line, int dayOfWeek) {
        // example: "19904,0,0,0,0,0,0,1,20191015,20191019"
        final String[] values = line.split(",");
        return "1".equals(values[dayOfWeek]);
    }

    private static boolean lineContainsDate(final String line, final String date) {
        // example: "19904,0,0,0,0,0,0,1,20191015,20191019"
        final String[] values = line.split(",");
        final String dateFrom = values[8];
        final String dateTo = values[9];
        final String properDate = date.replaceAll("-", "");
        boolean result = ((properDate.compareTo(dateFrom) >= 0) && (properDate.compareTo(dateTo) <= 0));
        return result;
    }


    public String getDirectoryOfGtfsFile() {
        return directoryOfGtfsFile;
    }

    public void setDirectoryOfGtfsFile(String directoryOfGtfsFile) {
        this.directoryOfGtfsFile = directoryOfGtfsFile;
    }
}
