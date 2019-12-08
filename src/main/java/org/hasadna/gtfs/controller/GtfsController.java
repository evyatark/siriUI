package org.hasadna.gtfs.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vavr.Tuple;
import io.vavr.collection.*;
import io.vavr.collection.List;
import org.hasadna.gtfs.Spark;
import org.hasadna.gtfs.db.MemoryDB;
import org.hasadna.gtfs.entity.RawData;
import org.hasadna.gtfs.entity.StopsTimeData;
import org.hasadna.gtfs.repository.RawDataRepository;
import org.hasadna.gtfs.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import java.awt.*;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hasadna.gtfs.service.Utils.generateKey;

@RestController
public class GtfsController {

    private static Logger logger = LoggerFactory.getLogger(GtfsController.class);

    @Autowired
    Shapes shapesService;

    @Autowired
    StreamResults streamResults;

    @Autowired
    SiriData siriData;

    @Autowired
    Stops stops;

    @Autowired
    Routes gtfsRoutes;

    @Autowired
    Spark spark;

    @Autowired
    MemoryDB db;

    @Autowired
    ReadSiriRawData readSiriRawData;

    @Autowired
    RawDataRepository rawDataRepository;

    @Value("${tests.delete.prev.db.entry:false}")
    public boolean deletePreviousEntry;

    @PostConstruct
    public void init() {
        //fillValuesForMonth("8177");     // doing all hard work for route 8177 on August 1-15
    }


    @GetMapping("siri/route")   ///{routeId}/{date}")
    public String siriTripsOfRoute() {
        Instant start = Instant.now();
        streamResults.do1();
        Instant finish = Instant.now();
        long timeElapsed = Duration.between(start, finish).toMillis();
        return "OK, elapsed: " + timeElapsed;
    }

    @GetMapping(value = "siri/db/count/{date}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Long countSiriRawData(@PathVariable String date) {
        long count = rawDataRepository.countByDate(date);
        logger.info("counted {} rows for date {}", count, date);
        return count;
    }

    @GetMapping(value = "siri/delete/{date}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Long deleteByDate(@PathVariable String date) {
        return readSiriRawData.deleteAllOfDate(date);
    }

    @GetMapping(value = "siri/group1/{date}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String groupToTrips(@PathVariable String date) {
        return groupToTrips(date, 15530, 15530);
    }

    @GetMapping(value = "siri/group/{date}/{fromRouteId}/{toRouteId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String groupToTrips(
            @PathVariable String date, @PathVariable Integer fromRouteId, @PathVariable Integer toRouteId) {

        readSiriRawData.readEverything(date);
//        logger.info("===> siri/group/{}",date);
//        logger.info("from={}", fromRouteId);
//        logger.info("to={}", toRouteId);
//        Map<String, Map<String, Stream<String>>> result = siriData.groupLinesOfEachRoute(
//                //List.of("10811", "10812", "10801", "10802", "10804", "10805", "10806", "10807")
//                List.rangeClosed(fromRouteId, toRouteId).map(number -> Integer.toString(number))
//                , date);
//        logger.info("<=== siri/group/{}",date);
//        java.util.Map<String,java.util.Map<String, java.util.List<String>>> retVal = convertToJavaMaps(result);
//        return retVal;

        java.util.List<RawData> result = rawDataRepository.findByDate(date);
        logger.info("for date {} we have in DB {} lines", date, result.size());
        return Integer.toString(result.size());
    }

    private java.util.Map<String,java.util.Map<String, java.util.List<String>>> convertToJavaMaps(Map<String, Map<String, Stream<String>>> result) {
        java.util.HashMap mapRoutes = new java.util.HashMap<>();
        result.keySet().forEach(key -> {
            mapRoutes.put(key, convertToJava(result.get(key).get()));
        });
        return mapRoutes;
    }

    private java.util.Map<String, java.util.List<String>> convertToJava(Map<String, Stream<String>> map) {
        java.util.HashMap<String, java.util.List<String>> mapTrips = new java.util.HashMap<>();
        map.keySet().forEach(key -> {
            mapTrips.put(key, map.get(key).map(stream -> stream.toJavaList()).getOrElse(new ArrayList<>()));
        });
        return mapTrips;
    }


    private java.util.Map<String,java.util.Map<Integer, StopsTimeData>> convertToJavaMaps2(Map<String, Map<Integer, StopsTimeData>> result) {
        java.util.HashMap<String,java.util.Map<Integer, StopsTimeData>> mapRoutes = new java.util.HashMap<>();
        result.keySet().forEach(key -> {
            mapRoutes.put(key, convertToJava2(result.get(key).get()));
        });
        return mapRoutes;
    }

    private java.util.Map<Integer, StopsTimeData> convertToJava2(Map<Integer, StopsTimeData> result) {
        java.util.HashMap<Integer, StopsTimeData> mapStops = new java.util.HashMap<>();
        result.keySet().forEach(key -> {
            mapStops.put(key, result.getOrElse(key, null));
        });
        return mapStops;
    }


    /**
     * Assuming that the shape of a route does not change at all!
     * So no argument for date
     * @param routeId
     * @return
     */
    @GetMapping("gtfs/shape/{routeId}/{date}")
    public String retrieveShapeOfRouteAsJson(@PathVariable String routeId, @PathVariable String date) {
        logger.info("===> gtfs/shape/{}/{}",routeId,date);
        String result = "";

        final String key = generateKey("shape", routeId, date);
        if (deletePreviousEntry) {
            db.deleteShapeKey(key);
        }
        String fromDB = db.readKey(key);
        if (!deletePreviousEntry && (fromDB != null)) {
            result = fromDB;
        }
        else {

            result = shapesService.findShape(routeId, date);

            if (result != null) {
                db.writeKeyValue(key, result);
            }
        }
        logger.info("<=== gtfs/shape/{}/{}",routeId,date);
        return result;
        // sample
        // route 16212
        //return "[[31.785666,35.200639],[31.785614,35.200606],[31.785572,35.200588],[31.785520,35.200582],[31.785404,35.200578],[31.785334,35.200586],[31.785270,35.200584],[31.785097,35.200580],[31.784563,35.200537],[31.784459,35.200509],[31.784152,35.200438],[31.784050,35.200371],[31.784017,35.200417],[31.783750,35.200754],[31.783474,35.201158],[31.783210,35.201494],[31.783112,35.201640],[31.782950,35.201872],[31.782667,35.202277],[31.782437,35.202563],[31.782359,35.202678],[31.782450,35.202754],[31.782581,35.202863],[31.782839,35.203035],[31.783063,35.203189],[31.783415,35.203409],[31.783846,35.203736],[31.784036,35.203880],[31.784221,35.203985],[31.784422,35.204086],[31.784621,35.204152],[31.784850,35.204185],[31.785067,35.204176],[31.785096,35.204175],[31.785278,35.204139],[31.785482,35.204073],[31.785645,35.204019],[31.785748,35.203985],[31.786028,35.203903],[31.786215,35.203857],[31.786394,35.203837],[31.786548,35.203844],[31.786816,35.203881],[31.786988,35.203934],[31.787105,35.203978],[31.787214,35.204032],[31.787354,35.204108],[31.787437,35.204167],[31.787749,35.204421],[31.787902,35.204571],[31.787916,35.204411],[31.787927,35.204211],[31.787920,35.204005],[31.787864,35.203034],[31.787855,35.202004],[31.787844,35.201733],[31.787843,35.201708],[31.787840,35.201364],[31.787851,35.201194],[31.787868,35.201070],[31.787897,35.200944],[31.787918,35.200862],[31.787947,35.200778],[31.788019,35.200622],[31.788085,35.200491],[31.788134,35.200396],[31.788146,35.200284],[31.788078,35.200220],[31.788034,35.200169],[31.787903,35.200044],[31.787599,35.199714],[31.787404,35.199527],[31.787120,35.199246],[31.787099,35.199221],[31.787020,35.199137],[31.786725,35.198821],[31.786195,35.198233],[31.785871,35.197949],[31.785762,35.197836],[31.785730,35.197960],[31.785524,35.198352],[31.785443,35.198481],[31.785258,35.198629],[31.785137,35.198682],[31.785054,35.198688],[31.784935,35.198664],[31.784769,35.198583],[31.783344,35.197523],[31.782438,35.196818],[31.782364,35.196766],[31.782106,35.196578],[31.781623,35.196207],[31.780635,35.195471],[31.780489,35.195371],[31.780327,35.195275],[31.780284,35.195243],[31.780214,35.195196],[31.779373,35.194617],[31.779178,35.194493],[31.778938,35.194366],[31.778627,35.194211],[31.778438,35.194130],[31.777941,35.193975],[31.777812,35.193937],[31.777628,35.193890],[31.777453,35.193849],[31.777197,35.193799],[31.776905,35.193753],[31.776467,35.193721],[31.776044,35.193704],[31.775415,35.193767],[31.774780,35.193885],[31.771582,35.194533],[31.771262,35.194594],[31.770988,35.194660],[31.770353,35.194789],[31.769721,35.194852],[31.769584,35.194852],[31.769310,35.194838],[31.769057,35.194804],[31.768920,35.194786],[31.768730,35.194740],[31.768538,35.194670],[31.768345,35.194622],[31.768127,35.194544],[31.767584,35.194263],[31.767406,35.194158],[31.767275,35.194086],[31.767186,35.194032],[31.767060,35.193952],[31.766943,35.193880],[31.766332,35.193496],[31.766165,35.193400],[31.766006,35.193311],[31.765759,35.193204],[31.765597,35.193140],[31.765406,35.193070],[31.765235,35.193017],[31.765058,35.192968],[31.764903,35.192939],[31.764758,35.192923],[31.764506,35.192897],[31.764120,35.192897],[31.763955,35.192912],[31.763666,35.192961],[31.763407,35.193019],[31.763170,35.193092],[31.762568,35.193328],[31.762122,35.193507],[31.761956,35.193568],[31.761662,35.193652],[31.761430,35.193669],[31.761223,35.193692],[31.760964,35.193692],[31.760788,35.193684],[31.760469,35.193645],[31.760284,35.193615],[31.760082,35.193565],[31.759867,35.193497],[31.759569,35.193362],[31.759465,35.193305],[31.759320,35.193216],[31.759233,35.193164],[31.758903,35.192916],[31.758314,35.192457],[31.757520,35.191820],[31.757350,35.191670],[31.757167,35.191497],[31.757014,35.191370],[31.756848,35.191246],[31.756668,35.191119],[31.756480,35.191012],[31.756399,35.190989],[31.756289,35.190959],[31.756246,35.190951],[31.756191,35.190944],[31.756129,35.190940],[31.755987,35.190945],[31.755861,35.190963],[31.755579,35.191018],[31.755386,35.191045],[31.755253,35.191064],[31.755180,35.191055],[31.755093,35.191041],[31.755019,35.191021],[31.754915,35.190972],[31.754752,35.190896],[31.754397,35.190700],[31.754227,35.190602],[31.754105,35.190514],[31.753925,35.190361],[31.753765,35.190207],[31.753604,35.190031],[31.753216,35.189610],[31.753071,35.189468],[31.752843,35.189263],[31.752767,35.189176],[31.752671,35.189075],[31.752281,35.188798],[31.752179,35.188756],[31.752106,35.188733],[31.751863,35.188651],[31.751655,35.188585],[31.751496,35.188551],[31.751335,35.188525],[31.751177,35.188516],[31.751091,35.188509],[31.750873,35.188520],[31.750799,35.188524],[31.750546,35.188488],[31.750449,35.188456],[31.750403,35.188418],[31.750366,35.188358],[31.750353,35.188254],[31.750370,35.188121],[31.750402,35.187956],[31.750454,35.187725],[31.750496,35.187563],[31.750611,35.187272],[31.750690,35.187134],[31.750896,35.186665],[31.751070,35.186352],[31.751162,35.186170],[31.751333,35.185893],[31.751567,35.185557],[31.751596,35.185507],[31.751610,35.185509],[31.751651,35.185503],[31.751689,35.185482],[31.751712,35.185460],[31.751748,35.185381],[31.751740,35.185340],[31.751728,35.185308],[31.751715,35.185287],[31.751691,35.185263],[31.751663,35.185246],[31.751643,35.185241],[31.751622,35.185236],[31.751579,35.185234],[31.751569,35.185236],[31.751447,35.185162],[31.751144,35.185011],[31.750945,35.184962],[31.750753,35.184931],[31.750431,35.184931],[31.750185,35.184931],[31.749805,35.185004],[31.748866,35.185454],[31.748763,35.185492],[31.748756,35.185476],[31.748740,35.185457],[31.748729,35.185448],[31.748722,35.185440],[31.748679,35.185427],[31.748663,35.185427],[31.748649,35.185429],[31.748634,35.185434],[31.748621,35.185442],[31.748614,35.185446],[31.748601,35.185458],[31.748595,35.185465],[31.748590,35.185471],[31.748579,35.185487],[31.748575,35.185495],[31.748566,35.185514],[31.748559,35.185534],[31.748554,35.185554],[31.748551,35.185576],[31.748550,35.185587],[31.748549,35.185610],[31.748550,35.185625],[31.748446,35.185615],[31.748378,35.185601],[31.748287,35.185571],[31.748118,35.185477],[31.748087,35.185457],[31.748048,35.185432],[31.747955,35.185331],[31.747883,35.185232],[31.747813,35.185051],[31.747579,35.184056],[31.747502,35.183893],[31.747427,35.183766],[31.747434,35.183747],[31.747437,35.183730],[31.747440,35.183714],[31.747440,35.183697],[31.747438,35.183680],[31.747435,35.183663],[31.747430,35.183647],[31.747423,35.183632],[31.747415,35.183618],[31.747405,35.183606],[31.747394,35.183595],[31.747382,35.183585],[31.747369,35.183578],[31.747356,35.183572],[31.747341,35.183568],[31.747326,35.183567],[31.747165,35.183195],[31.747020,35.182978],[31.746820,35.182775],[31.746562,35.182584],[31.746066,35.182319],[31.745740,35.182061],[31.745176,35.181593],[31.744877,35.181080],[31.744701,35.180738],[31.744483,35.180158],[31.744285,35.179500],[31.744130,35.179076],[31.744043,35.178753],[31.743955,35.178453],[31.743900,35.178240],[31.743721,35.177521],[31.743643,35.177172],[31.743594,35.176992],[31.743446,35.176452],[31.743362,35.176194],[31.743332,35.176068],[31.743165,35.175712],[31.743081,35.175593],[31.742998,35.175446],[31.742842,35.175202],[31.742687,35.174727],[31.742532,35.174203],[31.742490,35.173980],[31.742488,35.173771],[31.742495,35.172358],[31.742507,35.171953],[31.742507,35.171611],[31.742547,35.171242],[31.742567,35.171066],[31.742602,35.170605],[31.742671,35.170132],[31.742710,35.169752],[31.742735,35.169519],[31.742760,35.169357],[31.742773,35.169057],[31.742829,35.168529],[31.742865,35.168159],[31.742888,35.167711],[31.742928,35.167126],[31.742948,35.166607],[31.742986,35.166148],[31.743019,35.165168],[31.743084,35.164714],[31.743201,35.164203],[31.743299,35.163948],[31.743507,35.163663],[31.743867,35.163323],[31.744308,35.163077],[31.744559,35.162909],[31.744749,35.162766],[31.744911,35.162576],[31.745050,35.162372],[31.745166,35.162155],[31.745259,35.161924],[31.745317,35.161612],[31.745422,35.160557],[31.745502,35.160077],[31.745583,35.159453],[31.745675,35.158801],[31.745699,35.158163],[31.745660,35.157866],[31.745594,35.157593],[31.745526,35.157402],[31.745420,35.157172],[31.745199,35.156860],[31.744832,35.156576],[31.743951,35.155901],[31.743960,35.155891],[31.743967,35.155881],[31.743975,35.155864],[31.743979,35.155852],[31.743982,35.155840],[31.743982,35.155827],[31.743982,35.155815],[31.743979,35.155800],[31.743974,35.155785],[31.743966,35.155768],[31.743958,35.155758],[31.743946,35.155745],[31.743931,35.155735],[31.743920,35.155730],[31.743903,35.155726],[31.743891,35.155725],[31.743880,35.155725],[31.743863,35.155729],[31.743846,35.155735],[31.743836,35.155742],[31.743827,35.155749],[31.743815,35.155763],[31.743808,35.155773],[31.743801,35.155789],[31.743178,35.155295],[31.742780,35.155035],[31.742176,35.154738],[31.740545,35.153848],[31.739582,35.153292],[31.739129,35.152980],[31.738560,35.152464],[31.737841,35.151704],[31.737307,35.150944],[31.736761,35.150184],[31.736448,35.149777],[31.735356,35.148474],[31.734869,35.147877],[31.734416,35.147348],[31.733963,35.146887],[31.733464,35.146507],[31.733035,35.146249],[31.732675,35.146182],[31.732327,35.146168],[31.732048,35.146263],[31.730318,35.147399],[31.729740,35.147840],[31.729242,35.148451],[31.728859,35.148967],[31.728418,35.149931],[31.727723,35.151438],[31.727537,35.151696],[31.727340,35.151941],[31.727084,35.152159],[31.725151,35.153118],[31.724800,35.153341],[31.724579,35.153571],[31.724405,35.153870],[31.724278,35.154182],[31.724185,35.154522],[31.724162,35.154861],[31.724302,35.157305],[31.724361,35.158662],[31.724384,35.159667],[31.724307,35.160334],[31.723860,35.161513],[31.723677,35.161853],[31.723422,35.162151],[31.723067,35.162429],[31.722796,35.162627],[31.722529,35.162858],[31.722293,35.163217],[31.722193,35.163632],[31.722228,35.164365],[31.722298,35.164989],[31.722391,35.164985],[31.722563,35.165009],[31.722675,35.165041],[31.722750,35.165083],[31.722818,35.165125],[31.722870,35.165186],[31.722966,35.165464],[31.723092,35.166907],[31.723089,35.167018],[31.723074,35.167143],[31.722995,35.167545],[31.722916,35.167937],[31.722913,35.168003],[31.722910,35.168062],[31.722925,35.168186],[31.722977,35.168313],[31.723074,35.168397],[31.723176,35.168437],[31.723276,35.168464],[31.723382,35.168480],[31.723432,35.168386],[31.723476,35.168252],[31.723549,35.167939],[31.723649,35.167515],[31.723796,35.167022],[31.723883,35.166791],[31.723813,35.166723],[31.723748,35.166628],[31.723693,35.166520],[31.723672,35.166463],[31.723650,35.166373],[31.723607,35.166061],[31.723558,35.165686],[31.723635,35.165680],[31.723700,35.165669],[31.723785,35.165626],[31.723881,35.165574],[31.724066,35.165388],[31.724294,35.165147],[31.724536,35.164933],[31.724916,35.164693],[31.725291,35.164514],[31.725482,35.164410],[31.725558,35.164358],[31.725688,35.164259],[31.725698,35.164269],[31.725709,35.164278],[31.725722,35.164285],[31.725736,35.164289],[31.725750,35.164289],[31.725764,35.164286],[31.725777,35.164281],[31.725788,35.164274],[31.725800,35.164262],[31.725809,35.164249],[31.725816,35.164234],[31.725820,35.164218],[31.725821,35.164202],[31.725820,35.164186],[31.725816,35.164170],[31.725810,35.164155],[31.725802,35.164142],[31.725791,35.164130],[31.725779,35.164122],[31.725766,35.164116],[31.725753,35.164113],[31.725738,35.164113],[31.725725,35.164116],[31.725714,35.164120],[31.725707,35.164124],[31.725700,35.164130],[31.725692,35.164138],[31.725686,35.164146],[31.725678,35.164160],[31.725675,35.164170],[31.725672,35.164180],[31.725670,35.164191],[31.725670,35.164202],[31.725671,35.164213],[31.725673,35.164224],[31.725676,35.164234],[31.725680,35.164244],[31.725688,35.164259],[31.725558,35.164358],[31.725482,35.164410],[31.725291,35.164514],[31.724916,35.164693],[31.724536,35.164933],[31.724294,35.165147],[31.724066,35.165388],[31.723881,35.165574],[31.723785,35.165626],[31.723700,35.165669],[31.723635,35.165680],[31.723558,35.165686],[31.723607,35.166061],[31.723650,35.166373],[31.723672,35.166463],[31.723693,35.166520],[31.723748,35.166628],[31.723813,35.166723],[31.723883,35.166791],[31.724143,35.166419],[31.724581,35.165803],[31.724887,35.165511],[31.725111,35.165356],[31.725234,35.165301],[31.725313,35.165286],[31.725461,35.165332],[31.725513,35.165364],[31.725578,35.165418],[31.725609,35.165452],[31.725643,35.165493],[31.725646,35.165499],[31.725634,35.165507],[31.725626,35.165514],[31.725615,35.165527],[31.725610,35.165537],[31.725603,35.165553],[31.725600,35.165564],[31.725598,35.165576],[31.725598,35.165594],[31.725599,35.165605],[31.725602,35.165617],[31.725608,35.165633],[31.725614,35.165643],[31.725624,35.165656],[31.725632,35.165664],[31.725640,35.165670],[31.725653,35.165677],[31.725650,35.165686],[31.725489,35.165825],[31.725338,35.165963],[31.725203,35.166086],[31.725118,35.166174],[31.724955,35.166344],[31.724765,35.166580],[31.724576,35.166894],[31.724323,35.167416],[31.724254,35.167630],[31.724187,35.167859],[31.724096,35.168490],[31.724030,35.169103],[31.724021,35.169405],[31.723437,35.169548],[31.722891,35.169682],[31.722575,35.169789],[31.722569,35.169791],[31.722450,35.169861],[31.722404,35.169906],[31.722318,35.170015],[31.722166,35.170270],[31.722150,35.170330],[31.722063,35.170648],[31.722043,35.170719],[31.721963,35.171011],[31.721940,35.171096],[31.721895,35.171262],[31.721696,35.171243],[31.721687,35.171246],[31.721501,35.171294],[31.721360,35.171510],[31.721354,35.171520],[31.721309,35.171652],[31.721281,35.171809],[31.721200,35.172344],[31.721195,35.172355],[31.720959,35.172290],[31.720813,35.172272],[31.720705,35.172259]]";
    }

//    private String generateKey(String dataKind, String routeId, String date) {
//        return dataKind + "$" + routeId + "@" + date;
//    }


    @GetMapping("gtfs/stops/{routeId}/{tripId}/{date}")
    //List<TripData> tripsData
    public java.util.Map<String, java.util.Map<Integer, StopsTimeData>> generateStopsMap2(@PathVariable final String routeId, @PathVariable final String tripId, @PathVariable final String date ) {
        logger.info("===> gtfs/stops/{}/{}/{}",routeId, tripId, date);
        try {
            Map result = stops.generateStopsMap1(HashSet.of(tripId), date);
            if (!result.isEmpty()) {
                return convertToJavaMaps2(result);
            } else {  // empty - usual method of searching tripId in GTFS stops_time.txt failed.
                java.util.Map<String, java.util.Map<Integer, StopsTimeData>> res = new java.util.HashMap<>();
                return res;
            }
        }
        finally {
            logger.info("<=== gtfs/stops/{}/{}/{}",routeId, tripId, date);
        }
    }


    public java.util.Map<String, java.util.Map<Integer, StopsTimeData>> generateStopsMap2(final TripData tripData, final String date ) {
        String tripId = tripData.getSiriTripId();
        Map result = stops.generateStopsMap1(HashSet.of(tripId), date);
        if (!result.isEmpty()) {
            return result.toJavaMap();
        }
        // empty - usual method of searching tripId in GTFS stops_time.txt failed.
        if (tripData.getAlternateTripId() != null) {
            result = stops.generateStopsMap1(HashSet.of(tripData.getAlternateTripId()), date);
            if (!result.isEmpty()) {
                return result.toJavaMap();
            }
        }
        Map<Integer, StopsTimeData> stopsTimeDataMap = findAlternateTripId(tripData.getRouteId(), tripData.getOriginalAimedDeparture(), date);
        return HashMap.of(tripId, stopsTimeDataMap.toJavaMap()).toJavaMap();
    }

    private Map<Integer, StopsTimeData> findAlternateTripId(String routeId, String originalAimedDeparture, String date) {
        //in trips.txt search: grep "^15532," trips.txt
        // (15532 is the routeId)
        // from result, take unique serviceIds (the second value in each line)
        // for each serviceId, search: grep 19902 calendar.txt
        // (where 19902 is the serviceId)
        // result is (for example): 19902,0,0,1,1,1,0,0,20191015,20191019
        // from all results, choose the one that:
        // a. date arg is inside date range (,20191015,20191019)
        // b. day of week of date arg has 1 in (,0,0,1,1,1,0,0,)
        // there should be only one such result.
        // (assuming the result has serviceId 19902) now search: grep "^15532,19902" trips.txt
        // result is:
        /*
15532,19902,41569028_151019,,1,106264
15532,19902,41569029_151019,,1,106264
15532,19902,41569030_151019,,1,106264
15532,19902,41569031_151019,,1,106264
15532,19902,41569032_151019,,1,106264
15532,19902,41569033_151019,,1,106264
15532,19902,41569034_151019,,1,106264
15532,19902,41569035_151019,,1,106264
15532,19902,41569036_151019,,1,106264
15532,19902,41569037_151019,,1,106264
15532,19902,41569038_151019,,1,106264
15532,19902,41569039_151019,,1,106264
15532,19902,41569040_151019,,1,106264
15532,19902,41569041_151019,,1,106264
15532,19902,41569042_151019,,1,106264
15532,19902,41569043_151019,,1,106264
15532,19902,41569044_151019,,1,106264
15532,19902,41569045_151019,,1,106264
15532,19902,41569046_151019,,1,106264
         */
        // so 41569028_151019, 41569029_151019, ... are the tripIds for that day
        // now you should match a OAD to each tripId:
        // for each tripId you search:
        // grep '^41569028_151019' stop_times.txt
        // and choose the line that ends with ",0"
        //41569028_151019,07:50:00,07:50:00,36782,1,0,1,0
        // the OAD is 07:50:00
    return null;
    }

    @GetMapping("gtfs/stops/{tripId}/{date}")
    public Map<String, Map<Integer, StopsTimeData>> generateStopsMap1(@PathVariable final String tripId, @PathVariable final String date ) {
        logger.info("===> gtfs/stops/{}/{}", tripId, date);
        try {
            return stops.generateStopsMap1(HashSet.of(tripId), date);
        }
        finally {
            logger.info("<=== gtfs/stops/{}/{}", tripId, date);
        }

    }

    @GetMapping("siri/trips/full/{routeId}/{date}")
    public java.util.List<TripData> siriFullTripData(@PathVariable final String routeId, @PathVariable final String date) {
        logger.info("===> siri/trips/full/{}/{}", routeId, date);
        try {
            Map<String, io.vavr.collection.Stream<String>> trips = siriData.findAllTrips(routeId, date);
            //java.util.List<TripData> data = siriData.buildTripData(trips, date, routeId);
            List<TripData> fullData = siriData.buildFullTripsData2(trips, date, routeId);
            return fullData.toJavaList();
        }
        finally {
            logger.info("<=== siri/trips/full/{}/{}", routeId, date);
        }
    }

    @GetMapping("gtfs/trips/data/{routeId}/{date}")
    public java.util.List<TripData> gtfsTripData(@PathVariable final String routeId, @PathVariable final String date) {
        logger.info("===> gtfs/trips/data/{}/{}", routeId, date);
        try {
            List<TripData> data = siriData.buildTripData(date, routeId);
            return data.toJavaList();
        }
        finally {
            logger.info("<=== gtfs/trips/data/{}/{}", routeId, date);
        }
    }

    @GetMapping("siri/trips/data/{routeId}/{date}")
    public java.util.List<TripData> siriTripData(@PathVariable final String routeId, @PathVariable final String date) {
        logger.info("===> siri/trips/data/{}/{}", routeId, date);
        try {
            Map<String, io.vavr.collection.Stream<String>> trips = siriData.findAllTrips(routeId, date);
            List<TripData> data = siriData.buildFullTripsDataWithoutSiri(date, routeId);
                    //.buildTripData_orig(trips, date, routeId);
            return data.toJavaList();
        }
        finally {
            logger.info("<=== siri/trips/data/{}/{}", routeId, date);
        }
    }

    @GetMapping("siri/trips/short/{routeId}/{date}")
    public Object findAllTrips(@PathVariable final String routeId, @PathVariable final String date) throws IOException {
        logger.info("===> siri/trips/short/{}/{}", routeId, date);
        try {
            String result = "{\"route\": " + routeId + ", \"date\": \"" + date + "\", \"trips\": [";
            Map<String, io.vavr.collection.Stream<String>> trips = siriData.findAllTrips(routeId, date);
            for (String tripId : trips.keySet()) {
                long numberOfLines = trips.get(tripId).getOrElse(Stream.empty()).count(line -> true);
                result = result + "{\"tripId\": " + tripId + ", \"lines\": " + numberOfLines + "},";
            }
            // remove last ","
            result = result.substring(0, result.length() - 1);
            result = result + "]}";
            return ( new ObjectMapper() ).readValue(result, Object.class);
        }
        finally {
            logger.info("<=== siri/trips/short/{}/{}", routeId, date);
        }
    }

    @GetMapping("siri/day/{routeId}/{date}")
    public String retrieveSiriAndGtfsDataForRouteAndDateAsJson(@PathVariable String routeId, @PathVariable String date) {
        return retrieveSiriAndGtfsDataForRouteAndDateAsJson(routeId, date, true);
    }

    @GetMapping("siri/day/{routeId}/{date}/{withSiri}")
    public String retrieveSiriAndGtfsDataForRouteAndDateAsJson(@PathVariable String routeId, @PathVariable String date, @PathVariable Boolean withSiri) {
        String DATA_KIND = "siri";
        ///////////////////////////
        // here specifically hard code the value false
        // (but in general it should be true)
        ///////
        //boolean withReadingSiriLogs = false;
        boolean withReadingSiriLogs = withSiri;
        //
        //////////////
        logger.info("===> siri/day/{}/{}",routeId,date);

        if (!withReadingSiriLogs) {
            DATA_KIND = "gtfs";
        }
        final String key = generateKey(DATA_KIND, routeId, date);
        if (deletePreviousEntry) {
            db.deleteSiriKey(key);
        }
        String fromDB = db.readKey(key);
        if ((fromDB != null) && !"[]".equals(fromDB)) {
            logger.debug("found value for key {} in memoryDB", key);
            logger.info("<=== siri/day/{}/{}",routeId,date);
            return fromDB;
        }
        logger.debug("key {} not found in memory DB");



        // if false - the results will not contain Siri points of the bus.
        // Only shape, and stops - These are taken from GTFS. But we save a lot of time by not reading siri logs
        String result = siriData.dayResults(routeId, date, withReadingSiriLogs);

        if (result != null) {
            logger.info("writing value of key {} to memoryDB", key);
            db.writeKeyValue(key, result);
        }

        logger.info("<=== siri/day/{}/{} return json of {} characters",routeId,date, result.length());
        return result;
    }

    // In general this part is done offline by another application.
    // It fills tha HaloDB with data processed from the raw data
    public void fillValuesForMonth(String routeId) {
        List<String> dates = List.empty();
        for (int i = 1 ; i < 16 ; i++) {
            String s = Integer.toString(i);
            if (s.length() == 1) {
                s = "0" + s ;
            }
            dates.append("2019-08-" + s);
        }
        for (String date : dates) {
            retrieveSiriAndGtfsDataForRouteAndDateAsJson(routeId, date);
            logger.info("done filling {}", date);
        }
    }

    @GetMapping("gtfs/lines/{date}")
    public String retrieveAllLinesByDate(@PathVariable String date) {
        logger.info("===> gtfs/lines/{}",date);
        String json = null;
        try {
            logger.info("finding all GTFS routes for date {} ...", date);
            json = gtfsRoutes.allRoutesAsJson(date);    // this call is cached`
            logger.info("<=== gtfs/lines/{}",date);
        } catch (JsonProcessingException e) {
            logger.error("exception while converting to JSON", e);
            return "[]";
        }
        return json;
    }


    @GetMapping("siri/trips/{routeId}/{date}")
    public String retrieveAllTripsFromSiri(@PathVariable String routeId, @PathVariable String date) throws JsonProcessingException {
        String json = "";
        logger.info("===> siri/trips/{}",date);
        Map<String, Stream<String>> trips = siriData.findAllTrips(routeId, date);
        json = convertToJson(trips);
        logger.info("<=== siri/trips/{}",date);
        return json;
    }

    private String convertToJson(Map<String, Stream<String>> trips) throws JsonProcessingException {
        logger.debug("converting to JSON...");
        List<ShortTrip> tripsSummary =
                trips
                        .keySet()
                        .map(key -> Tuple.of(key,
                                Utils.extractAimedDeparture(trips.getOrElse(key, Stream.empty()).head()),
                                trips.getOrElse(key, Stream.empty()).size())  )
                        .map(tup -> ShortTrip.of(tup._1, tup._2, tup._3))
                        .toList();
        ObjectMapper x = new ObjectMapper();
        String json = x.writeValueAsString(tripsSummary);
        logger.debug("                  ... Done");
        logger.trace(json.substring(0, Math.min(3000, json.length())));
        logger.info("return json (size={} characters)", json.length());
        return json;
    }

    @GetMapping("gtfs/tripIdToDate/{routeId}/{date}")
    public String retrieveTripsOfRouteFromGtfsTripIdToDate(@PathVariable String routeId, @PathVariable String date) throws JsonProcessingException {
        String json = "";
        logger.info("===> gtfs/tripIdToDate/{}/{}",routeId,date);
        List<TripData> trips = siriData.buildTripsFromTripIdToDate(routeId, date);
        json = convertToJson(trips);
        logger.info("<=== gtfs/trips/{}",date);
        return json;
    }



    private String convertToJson(List<TripData> trips) throws JsonProcessingException {
        // shorten the Json:
        List<ShortTrip> shortTrips = minimize(trips);
        logger.debug("converting to JSON...");
        ObjectMapper x = new ObjectMapper();
        String json = x.writeValueAsString(shortTrips);
        logger.debug("                  ... Done");
        logger.trace(json.substring(0, Math.min(3000, json.length())));
        logger.info("return json (size={} characters)", json.length());
        return json;
    }

    private List<ShortTrip> minimize(List<TripData> trips) {
        return trips
                .map(tripData -> ShortTrip.of(tripData.getSiriTripId(), tripData.getOriginalAimedDeparture()));
    }





    public static class ShortTrip {
        String tripId;
        String hour;
        int gpsPoints = 0;

        public static ShortTrip of(String tripId, String hour) {
            ShortTrip x = new ShortTrip();
            x.hour = hour;
            x.tripId = tripId;
            return x;
        }

        public static ShortTrip of(String tripId, String hour, int gpsPoints) {
            ShortTrip x = new ShortTrip();
            x.hour = hour;
            x.tripId = tripId;
            x.gpsPoints = gpsPoints;
            return x;
        }

        public String getTripId() {
            return tripId;
        }

        public void setTripId(String tripId) {
            this.tripId = tripId;
        }

        public String getHour() {
            return hour;
        }

        public void setHour(String hour) {
            this.hour = hour;
        }

        public int getGpsPoints() {
            return gpsPoints;
        }

        public void setGpsPoints(int gpsPoints) {
            this.gpsPoints = gpsPoints;
        }
    }



    // ===== temporary =====


    @GetMapping("test")
    public String test() {
        logger.info("test API called");
        return "Hello World!";
    }

    @PostMapping("siri/raw/data")
    public String test(@RequestBody String body) {
        logger.info("received siri raw data {}", body);
        return "ok";
    }


    @GetMapping("shapes")
    public String calcShapesOnDate(@RequestParam String date, @RequestParam String routeIds) {
        logger.info("shapes for date={} routeIds={}", date, routeIds);
        long start = System.nanoTime();
        List<String> routes = List.of( routeIds.split(",") );
        for (String routeId : routes) {
            logger.info("retrieve shape for {} ...", routeId);
            retrieveShapeOfRouteAsJson(routeId, date);
        }
        long time = (System.nanoTime() - start)/1000000000 ;
        return "retrieved shapes for routes " + routeIds + " on date " + date + " in " + time + " seconds";
    }


    @GetMapping("shapesForAgency")
    public String calcShapesForAllRoutesOfAgencyOnDate(@RequestParam String date, @RequestParam String agency) throws IOException {

        String json = retrieveAllLinesByDate(date); // json looks like this: [{"routeId":"1","agencyCode":"25","shortName":"1","from":"ת. רכבת יבנה מערב-יבנה","to":"ת. רכבת יבנה מזרח-יבנה"},{"routeId":"2","agencyCode":"25","shortName":"1","from":"ת. רכבת יבנה מזרח-יבנה","to":"ת. רכבת יבנה מערב-יבנה"},

        ObjectMapper x = new ObjectMapper();
        java.util.List<java.util.LinkedHashMap<String, String>> objs = x.readValue(json, java.util.List.class);
        List<String> routeIds = List.empty();
        for (java.util.LinkedHashMap<String, String> obj : objs) {
            java.util.LinkedHashMap<String, String> map = obj;
            if (map.containsKey("agencyCode") && map.containsKey("routeId")) {
                if (map.get("agencyCode").equals(agency)) {
                    //logger.info("{}", map.get("routeId"));
                    routeIds.append(map.get("routeId"));
                }
            }
        }
        logger.info("found routes: " + routeIds);
        String routeIdsAsOneStr = routeIds.collect(Collectors.joining(","));
        logger.info("retreiving shapes...");
        String resultSummary = calcShapesOnDate(date, routeIdsAsOneStr);
        logger.info(resultSummary);
        return "retrieved shapes for routes " + routeIdsAsOneStr;
    }


    @GetMapping("siriForDateAndRouteFromTo/{date}/{fromRouteId}/{toRouteId}")
    public String routesForDate(@PathVariable String date, @PathVariable String fromRouteId, @PathVariable String toRouteId) {
        List<Integer> routes = Stream.rangeClosed(Integer.parseInt(fromRouteId), Integer.parseInt(toRouteId)).toList();
        String routeIds = routes.map(i -> Integer.toString(i)).collect(java.util.stream.Collectors.joining(","));
        //String routeIds = routes.toString();

        return routesForDate(date, routeIds);
    }
    @GetMapping("siriForDateAndRoutes/{date}")
    public String routesForDate(@PathVariable String date, @RequestParam String routeIds) {
        // for BS Routes use: 16211,16212,15540,15541,15494,15495,15491,8482,15489,15490,15487,15488,15485,15437,15444,15440,15441,15442,15443,15438,15439,8477,8480,15523,15524,15525,15526,15544,15545,15527,15528,15552,15553,15529,15530,16066,16067,15531,15532,6660,6661,6656
        List<String> routes = List.of( routeIds.split(",") );
        for (String routeId : routes) {
            long start = System.nanoTime();
            retrieveSiriAndGtfsDataForRouteAndDateAsJson(routeId, date);
            long time = (System.nanoTime() - start)/1000000000 ;
            logger.info("retrieved full siri for route {} on date {} in {} seconds.", routeId, date, time);
        }

        return "OK";
    }

    @GetMapping("siriForAgency/{date}/{agency}")
    public String calcSiriForAllRoutesOfAgencyOnDate(@PathVariable String date, @PathVariable String agency) throws IOException {

        String json = retrieveAllLinesByDate(date); // json looks like this: [{"routeId":"1","agencyCode":"25","shortName":"1","from":"ת. רכבת יבנה מערב-יבנה","to":"ת. רכבת יבנה מזרח-יבנה"},{"routeId":"2","agencyCode":"25","shortName":"1","from":"ת. רכבת יבנה מזרח-יבנה","to":"ת. רכבת יבנה מערב-יבנה"},

        ObjectMapper x = new ObjectMapper();
        List<Object> objs = x.readValue(json, List.class);
        List<String> routeIds = List.empty();
        for (Object obj : objs) {
            java.util.LinkedHashMap<String, String> map = (java.util.LinkedHashMap)obj;
            if (map.containsKey("agencyCode") && map.containsKey("routeId")) {
                if (map.get("agencyCode").equals(agency)) {
                    //logger.info("{}", map.get("routeId"));
                    routeIds.append(map.get("routeId"));
                }
            }
        }
        logger.info("found routes: " + routeIds);
        String routeIdsAsOneStr = routeIds.collect(Collectors.joining(","));
        logger.info("retreiving siri...");
        String resultSummary = routesForDate(date, routeIdsAsOneStr);
        logger.info(resultSummary);
        return "retrieved siri for routes " + routeIdsAsOneStr;
    }

    @GetMapping("halt")
    public String halt() {
        System.exit(1);
        return "OK";
    }

    @GetMapping("gtfs/distance/calc/{fromlat}/{fromlon}/{tolat}/{tolon}/{routeId}/{date}")
    public String retrieveTripsOfRouteFromGtfsTripIdToDate(@PathVariable String fromlat, @PathVariable String fromlon,
                                                           @PathVariable String tolat, @PathVariable String tolon,
                                                           @PathVariable String routeId, @PathVariable String date
    ) throws JsonProcessingException {
        return retrieveTripsOfRouteFromGtfsTripIdToDate(fromlat, fromlon, tolat, tolon, routeId, date, "70", "2");
    }

    // using http://localhost:8080/gtfs/distance/calc/31.711458/34.990584/32.082706/34.797251/15544/2019-10-27/71/2
    // seems to be matching best the distances of GTFS between stops.
    // TODO check same values with shaply.project()
    @GetMapping("gtfs/distance/calc/{fromlat}/{fromlon}/{tolat}/{tolon}/{routeId}/{date}/{precision}/{method}")
    public String retrieveTripsOfRouteFromGtfsTripIdToDate(@PathVariable String fromlat, @PathVariable String fromlon,
                                                           @PathVariable String tolat, @PathVariable String tolon,
                                                           @PathVariable String routeId, @PathVariable String date,
                                                           @PathVariable String precision, @PathVariable String method
                                                           ) throws JsonProcessingException {
        String json = "";
        logger.info("===> gtfs/distance/calc/{}/{}/{}/{}/{}/{}",fromlat,fromlon, tolat,tolon, routeId,date);
        String distance = siriData.calcDistance(routeId, date, new String[]{fromlat, fromlon}, new String[]{tolat, tolon}, precision, method);
        json = distance;
        logger.info("<=== gtfs/distance/calc/{}/{}/{}/{}/{}/{}",fromlat,fromlon, tolat,tolon, routeId,date);
        return json;
    }

}