package org.hasadna.gtfs.service;

import io.vavr.collection.*;
import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.entity.StopData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReadZipFileTest {

    private static Logger logger = LoggerFactory.getLogger(ReadZipFileTest.class);

    @Test
    public void test1() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        Stream<String> lines = rz.readZipFile("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt");

        List<String> first20Lines =  lines.drop(1).take(20).toList();
        first20Lines.forEach(line -> logger.info(line) );
    }

    @Test
    public void test2() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        Stream<String> lines = rz.readZipFile("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt");

        // vavr streams
        io.vavr.collection.Stream <String> vLines = lines.collect(io.vavr.collection.Stream.collector());

        vLines.drop(1).take(20).forEach(line -> logger.info(line));
    }

    @Test
    public void test3() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        Stream<String> lines = rz.readZipFile("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt");

        // vavr streams
        io.vavr.collection.Stream <String> vLines = lines.collect(io.vavr.collection.Stream.collector());

        vLines.drop(1).take(20).map(line -> StopData.extractFrom(line)).forEach(stopData -> logger.info(stopData.toString()));
    }

    @Test
    public void test4() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        Stream<String> lines = rz.readZipFile("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt");

        // vavr streams
        io.vavr.collection.Stream <String> vLines =
                rz.readZipFileV("/home/evyatar/logs/work/2019-03/gtfs/gtfs2019-03-20.zip", "stops.txt")
                       ;// .collect(io.vavr.collection.Stream.collector());

        int sizeAndLog = vLines
                .drop(1)
                .map(line -> StopData.extractFrom(line))
                .filter(stopData -> stopData.zone_id.equals("469"))
                .map(stopData -> {
                    logger.info(stopData.toString());
                    return stopData;
                } )
                .size();

        Assertions.assertThat(sizeAndLog).isEqualTo(34);
    }

    @Test
    public void test5() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        Stream<String> lines = rz.readZipFile("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt");

        // vavr streams
        io.vavr.collection.Stream <String> vLines =
                rz.readZipFileV("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt")
                        .collect(io.vavr.collection.Stream.collector());

        int sizeAndLog = vLines
                .drop(1)
                //.take(200)
                .map(line -> StopData.extractFrom(line))
                .filter(stopData -> stopData.stop_desc.contains("בית שמש"))
                .map(stopData -> {
                            logger.info(stopData.toString());
                            return stopData;
                        } )
                .size();
        Assertions.assertThat(sizeAndLog).isEqualTo(352);
    }


    @Test
    public void test6() throws IOException {
        ReadZipFile rz = new ReadZipFile();

        int sizeAndLog =
            io.vavr.collection.List.of("gtfs2019-03-20.zip","gtfs2019-03-21.zip","gtfs2019-03-22.zip","gtfs2019-03-23.zip")
                .flatMap(fileName -> {
                            try {
                                return rz.readZipFileV("/home/evyatar/logs/work/2019-03/gtfs/" + fileName, "stops.txt")
                                        .collect(io.vavr.collection.Stream.collector());
                            }
                            catch (Exception ex) {
                                return null;
                            }
                        }
                )
                .drop(1)
                .map(line -> StopData.extractFrom(line))
                .filter(stopData -> stopData.stop_desc.contains("בית שמש"))
                .map(stopData -> {
                    logger.info(stopData.toString());
                    return stopData;
                } )
                .size();
        Assertions.assertThat(sizeAndLog).isEqualTo(352*4);
    }


    @Test
    public void test7() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        io.vavr.collection.List x = io.vavr.collection.List.of("gtfs2019-03-20.zip","gtfs2019-03-21.zip");
        for (Object f : x) {

            String fileName = f.toString();
            io.vavr.collection.Stream<String> lines = null;
            try {
                lines = rz.readZipFileV("/home/evyatar/logs/work/2019-03/gtfs/" + fileName, "stops.txt")
                        .collect(io.vavr.collection.Stream.collector());
            } catch (Exception ex) {

            }

            int size = lines
                    .drop(1)
                    .map(line -> StopData.extractFrom(line))
                    .filter(stopData -> stopData.stop_desc.contains("בית שמש"))
                    .size();

            Assertions.assertThat(size).as("file " + fileName).isEqualTo(352);
        }
    }

//    public io.vavr.collection.Stream<String> stopLinesFromFile(String fileFullPath) {
//        io.vavr.collection.Stream<String> lines = null;
//        try {
//            lines = (new ReadZipFile())
//                        .readZipFileV(fileFullPath, "stops.txt")
//                        .collect(io.vavr.collection.Stream.collector());
//        } catch (Exception ex) {
//
//        }
//        return lines;
//    }

    @Test
    public void test8() throws IOException {
        Stream.rangeClosed(1, 9).forEach(i ->
            {
                String fileName = "/home/evyatar/logs/work/2019-04/gtfs/" + "gtfs2019-04-0" + i + ".zip";
                io.vavr.collection.Stream<String> lines = (new ReadZipFile()).stopLinesFromFile(fileName);

                if (lines != null ) {
                    int size = lines
                            .drop(1)    // headers in first row
                            .map(line -> StopData.extractFrom(line))
                            .filter(stopData -> stopData.stop_desc.contains("בית שמש"))
                            .size();
                    logger.info("{}: {}", fileName, size);
                    Assertions.assertThat(size).as("file " + fileName).isEqualTo(350);
                }
            });

    }

    @Test
    public void test9() throws IOException {
        Stream.rangeClosed(1, 9).forEach(i ->
        {
            String fileName = "/home/evyatar/logs/work/2019-04/gtfs/" + "gtfs2019-04-0" + i + ".zip";
            io.vavr.collection.Stream<String> lines = (new ReadZipFile()).stopLinesFromFile(fileName);

            if (lines != null ) {
                int size = lines
                        .drop(1)    // headers in first row
                        .map(line -> StopData.extractFrom(line))
                        .filter(stopData -> !stopData.stop_id.isEmpty())
                        .size();
                logger.info("{}: {}", fileName, size);
                Assertions.assertThat(size).as("file " + fileName).isGreaterThanOrEqualTo(27912);
            }
        });

    }

    @Test
    public void test10a() {
        ReadZipFile rzf = new ReadZipFile();
        List<String> lines = rzf.stopTimesLinesOfTripsFromFile(
                "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip",
                            List.of("41569051", "10021427")
                                    .toSet());
        Assertions.assertThat(lines.size()).isGreaterThan(0);
    }

    @Test
    public void test10() {
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        String fileFullPath = "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip";
        ReadZipFile rzf = new ReadZipFile();
        List<String> lines = List.empty();
        Set<String> tripIds = List.of("41569051", "10021427"
                //"41568956", "41568957", "41568958", "41568959"//, "41569020", "41569011", "41569023", "41569002", "41569014"   //, 41568960, 41569026, 41568950, 41568961, 41569005, 41568951, 41568962, 41569017, 41568952, 41568963, 41568953, 41568964, 41569008, 41568954, 41568965, 41568955, 41568966, 41568999
                ).toSet();
        Set<String> tripsInThisFile = HashSet.empty();
        int counter = 0 ;
        try {
            lines = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                    .filter(line -> lineBelongsToAnyTripId(tripIds, line)
            ).toList();
        } catch (Exception ex) {
            logger.error("exception", ex);
        }
        Assertions.assertThat(lines.size()).isGreaterThan(0);
    }

    private int count = 0 ;
    boolean lineBelongsToAnyTripId(final Set<String> tripIds, final String line) {
        // this works fine, but is a bit slow
        //return tripIds.stream().anyMatch(tripId -> line.startsWith(tripId + "_"));
        // this saves a few seconds:
        count ++;
        if (count%1000000 == 0) logger.info("{}", count);
        return tripIds.exists(id -> line.startsWith(id+"_"));
        //return tripIds.stream().anyMatch(tripId -> line.startsWith(tripId + "_"));
//        for (String tripId : tripIds) {
//            if (
//                    line.startsWith(tripId + "_") ||
//                            (line.startsWith(tripId) && tripId.contains("_"))
//            ) {
//                return true;
//            }
//        }
        // TODO check if following code improves performance:
        //String lineStart = line.split("_")[0];
        //return (tripIds.contains(lineStart));
//        return false;
    }

    @Test
    public void test11() throws IOException {
//        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
//        String fileFullPath = "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip";
//        ReadZipFile rzf = new ReadZipFile();
//        long count = 0 ;
//        Map<String, List<Long>> map = HashMap.empty();
//        Iterator<String> s = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
//                .drop(1)
//                //.take(1000000)
//                .map(line -> line.substring(0, line.indexOf(",")))
//                .iterator();
//        while (s.hasNext()) {
//            String tripId = s.next();
//            map = map.put(tripId, map.getOrElse(tripId, List.empty()).append(count++));
//            if (count%1000000 == 0) logger.info("{}", count);
//        }
// now I have map from tripId to list of all lines of that tripId
        Map<String, List<Long>> map = generateMapOfTextLines();
        logger.info("map size {}", map.keySet().size());
//        logger.info("1st element: {} - {}", map.keySet().head(), map.getOrElse(map.keySet().head(), List.empty()) );
//        List.of(1, 2, 3, 4, 5, 6,7 , 8, 9, 10, 11, 12, 13).forEach(index ->
//            logger.info("{} element: {} - {}", index, map1.keySet().toList().get(index), map1.getOrElse(map1.keySet().toList().get(index), List.empty()) )
//        );
        List<String> trips = List.of("13094993_151019","13683405_151019","20492801_151019","12342967_151019","14291354_161019");
        for (String tripId : trips) {
            //logger.info("lines for tripId {}: {}", tripId, map1.get(tripId));
            List<String> lines = getLinesOfTrip(tripId, map);
            if (!lines.isEmpty()) {
                logger.info("lines: ");
                lines.forEach(line -> logger.info("{}", line));
            }
        }
    }

    private Map<String, List<Long>> generateMapOfTextLines() throws IOException {
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        String fileFullPath = "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip";
        ReadZipFile rzf = new ReadZipFile();
        long count = 0 ;
        Map<String, List<Long>> map = HashMap.empty();
        Iterator<String> s = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                .drop(1)
                //.take(1000000)
                .map(line -> line.substring(0, line.indexOf(",")))
                .iterator();
        while (s.hasNext()) {
            String tripId = s.next();
            map = map.put(tripId, map.getOrElse(tripId, List.empty()).append(count++));
            if (count%1000000 == 0) logger.info("{}", count);
        }
        return map;
    }

    private List<String> getLinesOfTrip(String tripId, Map<String, List<Long>> map) throws IOException {
        String FILE_NAME_INSIDE_GTFS_ZIP = "stop_times.txt";
        String fileFullPath = "/home/evyatar/logs/gtfs/gtfs2019-10-16.zip";
        ReadZipFile rzf = new ReadZipFile();

        List<Long> list = map.getOrElse(tripId, List.empty()).sorted();
        logger.info("list of lines for trip {}: {}", tripId, list);
//        List<String> allLines = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP).drop(1).take(1000000).toList();
//        logger.info("{}",allLines.get(220110));
//        logger.info("{}",allLines.get(220111));
//        logger.info("{}",allLines.get(220112));
//        logger.info("{}",allLines.get(220113));
//        if (false) return List.empty();
        count = 0;
        List<String> lines = List.empty();
//        rzf = new ReadZipFile();
        Iterator<String> iter = rzf.readZipFile(fileFullPath, FILE_NAME_INSIDE_GTFS_ZIP)
                .drop(1)
                .iterator();
        while (iter.hasNext()) {
            String line = iter.next();
            if (list.head() == count) {
                //logger.info("count {} line {}", count, line);
                lines = lines.append(line);
                list = list.tail();
            }
            if (list.isEmpty()) break;
            count = count+1;
        }
//        logger.info("count={}", count);
//        logger.info("lines: ");
//        lines.forEach(line -> logger.info("{}", line));
        return lines;
    }
}
