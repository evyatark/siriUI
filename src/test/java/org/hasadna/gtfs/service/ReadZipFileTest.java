package org.hasadna.gtfs.service;

import org.assertj.core.api.Assertions;
import org.hasadna.gtfs.entity.StopData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ReadZipFileTest {

    private static Logger logger = LoggerFactory.getLogger(ReadZipFileTest.class);

    @Test
    public void test1() throws IOException {
        ReadZipFile rz = new ReadZipFile();
        Stream<String> lines = rz.readZipFile("/home/evyatar/logs/work/2019-03/gtfs/work 2019-03-20/gtfs2019-03-20.zip", "stops.txt");

        List<String> first20Lines =  lines.skip(1).limit(20).collect(Collectors.toList());
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
        IntStream.rangeClosed(1, 9).forEach(i ->
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
        IntStream.rangeClosed(1, 9).forEach(i ->
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
}
