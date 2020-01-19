package org.hasadna.gtfs.service;

import io.vavr.collection.List;
import io.vavr.control.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;

@Component
public class Shapes {

    private static Logger logger = LoggerFactory.getLogger(Shapes.class);

    @Value("${gtfsZipFileDirectory}")
    public String directoryOfGtfsFile;

    @Value("${tripIdToDate.ZipFileDirectory}")
    public String directoryOfMakatFile;

    @Autowired
    Shape shapeById;

    @Autowired
    TripFileReader tripFileReader;

//    private final String GTFS_DIR = "/home/evyatar/logs/work/work 2019-04-18/gtfs2019-04-18/";
//    private final String TRIPS_FILE = GTFS_DIR + "trips.txt";
//    private final String SHAPES_FILE = GTFS_DIR + "shapes.txt";

    private String findGtfsFileName(final String date) {
        String gtfsZipFileName = "gtfs" + date + ".zip";
        String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
        if (null == gtfsZipFileFullPath) {
            logger.warn("could not find GTFS file of date {}. Searched this path: {}", date, directoryOfGtfsFile);
            return null;
        }
        return gtfsZipFileFullPath;
    }

    private String findShapeId(final String routeId, final String date, final String gtfsZipFileFullPath) {
        //read trips file, find line that contains routeId
        // you get 2 lines but in both of them it is the same shape, so take the first.
        // from that line take 4th item (?) - this is the shape id
        Option<String> tripLine =
                List
                    .ofAll(tripFileReader.retrieveTripLinesFromFile(gtfsZipFileFullPath))
                    .filter(line -> line.startsWith(routeId + ","))
                    .headOption();

        logger.info("routeId={}, tripLine={}", routeId, tripLine.getOrElse("not found"));
        // format of tripLine: route_id,service_id,trip_id,trip_headsign,direction_id,shape_id

        Option<String> shapeIdOpt = tripLine
                .map(line -> line.split(","))   // split the line by commas, if tripLine has a value
                .map(arr -> arr[5]);                  // takes the 6th item from the array (if tripLine had a value)
        if (!shapeIdOpt.isDefined()) {
            logger.warn("could not find shapeId for route {}", routeId);
            return "[]";
        }
        String shapeId = shapeIdOpt.get();
        return shapeId;
    }


    @Cacheable("shapeByRouteAndDate")
    public String findShape(final String routeId, final String date) {
        try {
            String gtfsZipFileFullPath = findGtfsFileName(date);
            String shapeId = findShapeId(routeId, date, gtfsZipFileFullPath);

            java.util.List<String> shapeLines = shapeById.retrieveShapeId(shapeId, gtfsZipFileFullPath);
            logger.info("collected {} lines for shape {}", shapeLines.size(), shapeId);
            String json = generateShapeJson(shapeLines, shapeId);
            logger.debug("json for shape {}: {}", shapeId, json.substring(0, 2000));
            // [32.054065,35.239214][32.054393,35.239772][32.054735,35.240334]
            return json;
        }
        catch (Exception ex) {
            logger.error("during findShape, for routeId={}", routeId, ex);
            return "[]";
        }
    }

    private String generateShapeJson(java.util.List<String> shapeLines, String shapeId) {
        long start = System.currentTimeMillis();
        String shapePoints = "" + shapeLines.stream()
                .filter(line -> !StringUtils.isEmpty(line))
                .map(line -> {
                    String[] sp = line.split(",");
                    return "[" + sp[1] + "," + sp[2] + "]";
                })
                .reduce((a, b) -> a + "," + b).get();
        String result = "{\"shapeId\": \"" + shapeId + "\", \"shape\": [" + shapePoints + "]}";
        long end = System.currentTimeMillis();
        long duration = (end - start);
        if (duration >= 500) {
            // warn if it takes too long
            logger.info("generateShapeJson({}): duration= {} MilliSeconds", shapeId, duration);
        }
        return result;
    }


    // using trips.txt data to search tripIDs of the SUNDAY of that week!
//    public void findRouteTrips(String routeId, String date) throws IOException {
//        findLinesOfGtfsTripsFile(date).filter(line -> )
//    }

    private String calcName(String date) {
        String gtfsZipFileName = "gtfs" + date + ".zip";
        String gtfsZipFileFullPath = directoryOfGtfsFile + File.separatorChar + gtfsZipFileName;
        return gtfsZipFileFullPath;
    }

    private io.vavr.collection.Stream<String> findLinesOfGtfsTripsFile(String date) throws IOException {
        ReadZipFile rzf = new ReadZipFile();
        return rzf.readZipFileV(calcName(date), "trips.txt");
    }

    public List<String> retrieveShape(String routeId, String date) {

        List<String> empty = List.empty();

        try {

            ReadZipFile rzf = new ReadZipFile();
            String gtfsZipFileName = "gtfs" + date + ".zip";
            String gtfsZipFileFullPath = Utils.findFile(directoryOfGtfsFile, gtfsZipFileName);
            if (null == gtfsZipFileFullPath) {
                logger.warn("could not find GTFS file of date {}. Searched this path: {}", date, directoryOfGtfsFile);
                return empty;
            }
            Option<String> tripLine = rzf
                    .tripLinesFromFile(gtfsZipFileFullPath)
                    .filter(line -> line.startsWith(routeId + ",")).headOption();

            logger.info("routeId={}, tripLine={}", routeId, tripLine.getOrElse("not found"));

            Option<String> shapeIdOpt = tripLine
                    .map(line -> line.split(","))   // split the line by commas, if tripLine has a value
                    .map(arr -> arr[5]);                  // takes the 6th item from the array (if tripLine had a value)
            if (!shapeIdOpt.isDefined()) {
                logger.warn("could not find shapeId for route {}", routeId);
                return empty;
            }
            String shapeId = shapeIdOpt.get();
            java.util.List<String> shapeLines = shapeById.retrieveShapeId(shapeId, gtfsZipFileFullPath);
            return List.ofAll(shapeLines);
        }
        catch (Exception ex) {
            logger.error("during findShape, for routeId={}", routeId, ex);
            return empty;
        }
    }
}


        /*
                if ("q".equals(routeId)) {
            List<String> shapeLines = Arrays.asList(""
                    ,"106264,31.789159,35.202621,1"
                    ,"106264,31.789143,35.203053,2"
                    ,"106264,31.789143,35.203161,3"
                    ,"106264,31.789149,35.203580,4"
                    ,"106264,31.789167,35.203614,5"
                    ,"106264,31.789194,35.203638,6"
                    ,"106264,31.789302,35.203662,7"
                    ,"106264,31.789354,35.203669,8"
                    ,"106264,31.789442,35.203663,9"
                    ,"106264,31.789617,35.203652,10"
                    ,"106264,31.789585,35.203044,11"
                    ,"106264,31.789596,35.202872,12"
                    ,"106264,31.789620,35.202734,13"
                    ,"106264,31.789648,35.202720,15"
                    ,"106264,31.789692,35.202676,16"
                    ,"106264,31.789701,35.202659,17"
                    ,"106264,31.789709,35.202646,18"
                    ,"106264,31.789724,35.202600,19"
                    ,"106264,31.789730,35.202563,20"
                    ,"106264,31.789731,35.202512,21"
                    ,"106264,31.789726,35.202474,22"
                    ,"106264,31.789711,35.202428,23"
                    ,"106264,31.789863,35.202201,24"
                    ,"106264,31.790117,35.201824,25"
                    ,"106264,31.790363,35.201483,26"
                    ,"106264,31.790671,35.201166,27"
                    ,"106264,31.790695,35.201143,28"
                    ,"106264,31.790750,35.201031,29"
                    ,"106264,31.790726,35.200984,30"
                    ,"106264,31.790694,35.200927,31"
                    ,"106264,31.790610,35.200785,32"
                    ,"106264,31.790343,35.200504,33"
                    ,"106264,31.790322,35.200488,34"
                    ,"106264,31.790108,35.200319,35"
                    ,"106264,31.789949,35.200213,36"
                    ,"106264,31.789691,35.200078,37"
                    ,"106264,31.789621,35.200030,38"
                    ,"106264,31.789594,35.200009,39"
                    ,"106264,31.789558,35.199979,40"
                    ,"106264,31.789528,35.199946,41"
                    ,"106264,31.789508,35.199921,42"
                    ,"106264,31.789490,35.199886,43"
                    ,"106264,31.789479,35.199851,44"
                    ,"106264,31.789474,35.199822,45"
                    ,"106264,31.789472,35.199731,46"
                    ,"106264,31.789484,35.199634,47"
                    ,"106264,31.789491,35.199566,48"
                    ,"106264,31.789508,35.199459,49"
                    ,"106264,31.789534,35.199338,50"
                    ,"106264,31.789544,35.199270,51"
                    ,"106264,31.789618,35.199172,52"
                    ,"106264,31.789682,35.199072,53"
                    ,"106264,31.789789,35.198891,54"
                    ,"106264,31.789834,35.198814,55"
                    ,"106264,31.789949,35.198628,56"
                    ,"106264,31.790036,35.198481,57"
                    ,"106264,31.790167,35.198248,58"
                    ,"106264,31.790666,35.197472,59"
                    ,"106264,31.791039,35.196987,60"
                    ,"106264,31.791109,35.196892,61"
                    ,"106264,31.791131,35.196865,62"
                    ,"106264,31.791365,35.196593,63"
                    ,"106264,31.791662,35.196238,64"
                    ,"106264,31.791973,35.195921,65"
                    ,"106264,31.792280,35.195641,66"
                    ,"106264,31.793237,35.194798,67"
                    ,"106264,31.793303,35.194727,68"
                    ,"106264,31.793353,35.194624,69"
                    ,"106264,31.793410,35.194464,70"
                    ,"106264,31.793421,35.194300,71"
                    ,"106264,31.793408,35.194142,72"
                    ,"106264,31.793314,35.193734,73"
                    ,"106264,31.793276,35.193457,74"
                    ,"106264,31.793281,35.193279,75"
                    ,"106264,31.793310,35.193153,76"
                    ,"106264,31.793357,35.193038,77"
                    ,"106264,31.793433,35.192928,78"
                    ,"106264,31.793549,35.192856,79"
                    ,"106264,31.793657,35.192805,80"
                    ,"106264,31.793802,35.192780,81"
                    ,"106264,31.793979,35.192750,82"
                    ,"106264,31.794145,35.192733,83"
                    ,"106264,31.794308,35.192704,84"
                    ,"106264,31.794434,35.192678,85"
                    ,"106264,31.794557,35.192615,86"
                    ,"106264,31.794676,35.192541,87"
                    ,"106264,31.794767,35.192435,88"
                    ,"106264,31.794827,35.192319,89"
                    ,"106264,31.794871,35.192219,90"
                    ,"106264,31.794907,35.192091,91"
                    ,"106264,31.794951,35.191789,92"
                    ,"106264,31.794987,35.191501,93"
                    ,"106264,31.795037,35.191169,94"
                    ,"106264,31.795113,35.190887,95"
                    ,"106264,31.795221,35.190628,96"
                    ,"106264,31.795436,35.190101,97"
                    ,"106264,31.795588,35.189789,98"
                    ,"106264,31.795773,35.189402,99"
                    ,"106264,31.795827,35.189313,100"
                    ,"106264,31.795911,35.189173,101"
                    ,"106264,31.796032,35.189018,102"
                    ,"106264,31.796215,35.188802,103"
                    ,"106264,31.796424,35.188587,104"
                    ,"106264,31.796621,35.188431,105"
                    ,"106264,31.796906,35.188298,106"
                    ,"106264,31.796988,35.188262,107"
                    ,"106264,31.798189,35.187734,108"
                    ,"106264,31.798367,35.187638,109"
                    ,"106264,31.798459,35.187561,110"
                    ,"106264,31.798574,35.187433,111"
                    ,"106264,31.798688,35.187292,112"
                    ,"106264,31.798758,35.187133,113"
                    ,"106264,31.798821,35.186984,114"
                    ,"106264,31.798875,35.186817,115"
                    ,"106264,31.798941,35.186572,116"
                    ,"106264,31.798978,35.186470,117"
                    ,"106264,31.799020,35.186350,118"
                    ,"106264,31.799119,35.186121,119"
                    ,"106264,31.799254,35.185881,120"
                    ,"106264,31.799457,35.185638,121"
                    ,"106264,31.801254,35.184280,122"
                    ,"106264,31.801432,35.184067,123"
                    ,"106264,31.801583,35.183825,124"
                    ,"106264,31.801697,35.183571,125"
                    ,"106264,31.801790,35.183265,126"
                    ,"106264,31.801832,35.182920,127"
                    ,"106264,31.801843,35.182621,128"
                    ,"106264,31.801831,35.182367,129"
                    ,"106264,31.801803,35.182182,130"
                    ,"106264,31.801728,35.181834,131"
                    ,"106264,31.801669,35.181640,132"
                    ,"106264,31.801645,35.181562,133"
                    ,"106264,31.801552,35.181235,134"
                    ,"106264,31.801477,35.180908,135"
                    ,"106264,31.801410,35.180604,136"
                    ,"106264,31.801338,35.180288,137"
                    ,"106264,31.801263,35.179798,138"
                    ,"106264,31.801207,35.179417,139"
                    ,"106264,31.801170,35.178894,140"
                    ,"106264,31.801161,35.178306,141"
                    ,"106264,31.801160,35.177740,142"
                    ,"106264,31.801187,35.176765,143"
                    ,"106264,31.801149,35.176265,144"
                    ,"106264,31.801087,35.175823,145"
                    ,"106264,31.801036,35.175549,146"
                    ,"106264,31.800948,35.175262,147"
                    ,"106264,31.800927,35.175185,148"
                    ,"106264,31.800790,35.174841,149"
                    ,"106264,31.800673,35.174585,150"
                    ,"106264,31.800535,35.174314,151"
                    ,"106264,31.800376,35.174072,152"
                    ,"106264,31.800170,35.173814,153"
                    ,"106264,31.799922,35.173534,154"
                    ,"106264,31.799306,35.172883,155"
                    ,"106264,31.798732,35.172338,156"
                    ,"106264,31.797465,35.171086,157"
                    ,"106264,31.797065,35.170656,158"
                    ,"106264,31.796854,35.170397,159"
                    ,"106264,31.796672,35.170174,160"
                    ,"106264,31.796570,35.170028,161"
                    ,"106264,31.796375,35.169757,162"
                    ,"106264,31.796085,35.169343,163"
                    ,"106264,31.796041,35.169277,164"
                    ,"106264,31.795846,35.168999,165"
                    ,"106264,31.795703,35.168784,166"
                    ,"106264,31.795522,35.168489,167"
                    ,"106264,31.795234,35.167979,168"
                    ,"106264,31.795198,35.167914,169"
                    ,"106264,31.795011,35.167537,170"
                    ,"106264,31.794895,35.167248,171"
                    ,"106264,31.794772,35.166950,172"
                    ,"106264,31.794653,35.166569,173"
                    ,"106264,31.794553,35.166126,174"
                    ,"106264,31.794477,35.165675,175"
                    ,"106264,31.794434,35.165340,176"
                    ,"106264,31.794414,35.164962,177"
                    ,"106264,31.794417,35.164641,178"
                    ,"106264,31.794433,35.164315,179"
                    ,"106264,31.794450,35.164012,180"
                    ,"106264,31.794517,35.163538,181"
                    ,"106264,31.794559,35.163293,182"
                    ,"106264,31.794669,35.162844,183"
                    ,"106264,31.794755,35.162546,184"
                    ,"106264,31.794903,35.162093,185"
                    ,"106264,31.795122,35.161543,186"
                    ,"106264,31.795329,35.161107,187"
                    ,"106264,31.795603,35.160590,188"
                    ,"106264,31.796068,35.159549,189"
                    ,"106264,31.796283,35.159039,190"
                    ,"106264,31.796292,35.159013,191"
                    ,"106264,31.796513,35.158405,192"
                    ,"106264,31.796685,35.157836,193"
                    ,"106264,31.796826,35.157291,194"
                    ,"106264,31.796963,35.156669,195"
                    ,"106264,31.797069,35.156046,196"
                    ,"106264,31.797145,35.155561,197"
                    ,"106264,31.797200,35.155104,198"
                    ,"106264,31.797230,35.154589,199"
                    ,"106264,31.797245,35.153907,200"
                    ,"106264,31.797225,35.153332,201"
                    ,"106264,31.797179,35.152485,202"
                    ,"106264,31.797135,35.151630,203"
                    ,"106264,31.797106,35.151051,204"
                    ,"106264,31.797075,35.150387,205"
                    ,"106264,31.797097,35.149848,206"
                    ,"106264,31.797162,35.149238,207"
                    ,"106264,31.797208,35.148965,208"
                    ,"106264,31.797221,35.148903,209"
                    ,"106264,31.797431,35.147992,210"
                    ,"106264,31.797734,35.146651,211"
                    ,"106264,31.797775,35.146443,212"
                    ,"106264,31.797857,35.146075,213"
                    ,"106264,31.797924,35.145784,214"
                    ,"106264,31.797953,35.145652,215"
                    ,"106264,31.797965,35.145602,216"
                    ,"106264,31.798922,35.141357,217"
                    ,"106264,31.799250,35.139899,218"
                    ,"106264,31.799452,35.139000,219"
                    ,"106264,31.799492,35.138822,220"
            );
            return generateShapeJson(shapeLines, "106264");
        }
         */

